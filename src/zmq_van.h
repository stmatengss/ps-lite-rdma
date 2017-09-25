/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_ZMQ_VAN_H_
#define PS_ZMQ_VAN_H_
#include <zmq.h>
#include <stdlib.h>
#include <thread>
#include <string>
#include <atomic>
#include "ps/internal/van.h"
#include "libmrdma.h"
#if _MSC_VER
#define rand_r(x) rand()
#endif

namespace ps {
/**
 * \brief be smart on freeing recved data
 */
inline void FreeData(void *data, void *hint) {
  if (hint == NULL) {
    delete [] static_cast<char*>(data);
  } else {
    delete static_cast<SArray<char>*>(hint);
  }
}

/**
 * \brief ZMQ based implementation
 */
#define USE_RDMA 1


class ZMQVan : public Van {
 public:
  std::atomic<int> port_counter;
  const static int N = 10;
  const static int qp_depth = 100;
  const static int rdma_buffer_size = 1024;
  // static int port_num = 10086;
  m_ibv_res ibv_res[N];
  m_ibv_res ibv_res_recv[N];
  int recv_counter = 0;
  char *rdma_buffer[N];
  char *rdma_buffer_recv[N];
  std::thread connect_th[N];

  ZMQVan() { }
  virtual ~ZMQVan() { }

  void init_rdma() {
    port_counter = -1;
    for (int i = 0; i < 3; i ++ ) {
      ibv_res[i].ib_port = 1;
  // ibv_res[0].is_server = 1;
  // ibv_res[0].port = 10086;
      ibv_res[i].model = M_RC;

      rdma_buffer[i] = new char[rdma_buffer_size];

      m_open_device_and_alloc_pd(&ibv_res[i]);
      m_reg_buffer(&ibv_res[i], rdma_buffer[i], rdma_buffer_size);
      m_create_cq_and_qp(&ibv_res[i], qp_depth, IBV_QPT_RC);
    }
    for (int i = 0; i < 3; i ++ ) {
      ibv_res_recv[i].ib_port = 1;
  // ibv_res[0].is_server = 1;
  // ibv_res[0].port = 10086;
      ibv_res_recv[i].model = M_RC;

      rdma_buffer_recv[i] = new char[rdma_buffer_size];

      m_open_device_and_alloc_pd(&ibv_res_recv[i]);
      m_reg_buffer(&ibv_res_recv[i], rdma_buffer_recv[i], rdma_buffer_size);
      m_create_cq_and_qp(&ibv_res_recv[i], qp_depth, IBV_QPT_RC);
    }
    printf("==================Init Finish==================\n");
  }

 protected:
  void Start() override {
    // start zmq
  #if USE_RDMA == 0
    context_ = zmq_ctx_new();
    CHECK(context_ != NULL) << "create 0mq context failed";
    zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);
    // zmq_ctx_set(context_, ZMQ_IO_THREADS, 4);
    Van::Start();
  #else
    init_rdma();
    Van::Start();
  #endif

  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    
    Van::Stop();

  #if USE_RDMA == 0
    // close sockets
    int linger = 0;
    int rc = zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
    CHECK(rc == 0 || errno == ETERM);
    CHECK_EQ(zmq_close(receiver_), 0);
    for (auto& it : senders_) {
      int rc = zmq_setsockopt(it.second, ZMQ_LINGER, &linger, sizeof(linger));
      CHECK(rc == 0 || errno == ETERM);
      CHECK_EQ(zmq_close(it.second), 0);
    }
    zmq_ctx_destroy(context_);
  #else
    //XXX
  #endif
  }

  int Bind(const Node& node, int max_retry) override {

    int port = node.port;
    std::cout << "[Bind]" << node.DebugString() << std::endl;

  #if USE_RDMA == 0
    receiver_ = zmq_socket(context_, ZMQ_ROUTER);
    CHECK(receiver_ != NULL)
        << "create receiver socket failed: " << zmq_strerror(errno);
    int local = GetEnv("DMLC_LOCAL", 0);
    std::string addr = local ? "ipc:///tmp/" : "tcp://*:";
    unsigned seed = static_cast<unsigned>(time(NULL)+port);
    for (int i = 0; i < max_retry+1; ++i) {
      auto address = addr + std::to_string(port);
      if (zmq_bind(receiver_, address.c_str()) == 0) break;
      if (i == max_retry) {
        port = -1;
      } else {
        port = 10000 + rand_r(&seed) % 40000;
      }
    }
  #else 
    // Server side action
    for (int i = 0; i < 3; i ++ ) {
      ibv_res[i].is_server = 1;
      ibv_res[i].port = port + i;

      printf("[%d][%d][%d]Waiting\n", i, static_cast<int>(node.role), port + i );
      
      connect_th[i] = std::thread([&](m_ibv_res &ibv_res){

        m_sync(&ibv_res, "XXX", rdma_buffer[i]);
        m_modify_qp_to_rts_and_rtr(&ibv_res);
      }, std::ref(ibv_res[i]));
    }
    
    for (int i = 0; i < 3; i ++ ) {
      connect_th[i].join();
    }

  #endif

    return port;
  }

  void Connect(const Node& node) override {

    std::cout << "[Connect]" << node.DebugString() << std::endl;
    // debug_print("[%s][%d] Connext Begin!\n", node.hostname.c_str(), node.port);
  #if USE_RDMA == 0
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    int id = node.id;
    auto it = senders_.find(id);
    if (it != senders_.end()) {
      zmq_close(it->second);
    }
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) &&
        (node.id != my_node_.id)) {
      return;
    }
    void *sender = zmq_socket(context_, ZMQ_DEALER);
    CHECK(sender != NULL)
        << zmq_strerror(errno)
        << ". it often can be solved by \"sudo ulimit -n 65536\""
        << " or edit /etc/security/limits.conf";
    if (my_node_.id != Node::kEmpty) {
      std::string my_id = "ps" + std::to_string(my_node_.id);
      zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
    }
    // connect
    std::string addr = "tcp://" + node.hostname + ":" + std::to_string(node.port);
    if (GetEnv("DMLC_LOCAL", 0)) {
      addr = "ipc:///tmp/" + std::to_string(node.port);
    }
    if (zmq_connect(sender, addr.c_str()) != 0) {
      LOG(FATAL) <<  "connect to " + addr + " failed: " + zmq_strerror(errno);
    }
    senders_[id] = sender;
  #else 

    port_counter ++;

    ibv_res_recv[port_counter].is_server = 0;
    // printf("port_counter: %d\n", port_counter);
    ibv_res_recv[port_counter].port = node.port + port_counter;
    
    std::cout << std::this_thread::get_id() << std::endl;
    printf("[%d][%d]Waiting\n", static_cast<int>(node.role), ibv_res_recv[port_counter].port );

    m_sync(&ibv_res_recv[port_counter], node.hostname.c_str(), rdma_buffer_recv[port_counter]);
    // connect_th.join();

    m_modify_qp_to_rts_and_rtr(&ibv_res_recv[port_counter]);

    //Client side action
  #endif
  }

  int SendMsg(const Message& msg) override {
    std::lock_guard<std::mutex> lk(mu_);
    // find the socket
  #if USE_RDMA == 0
    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);
    auto it = senders_.find(id);
    if (it == senders_.end()) {
      LOG(WARNING) << "there is no socket to node " << id;
      return -1;
    }
    void *socket = it->second;

    // send meta
    int meta_size; char* meta_buf;
    PackMeta(msg.meta, &meta_buf, &meta_size);
    int tag = ZMQ_SNDMORE;
    int n = msg.data.size();
    if (n == 0) tag = 0;
    zmq_msg_t meta_msg;
    zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData, NULL);
    while (true) {
      if (zmq_msg_send(&meta_msg, socket, tag) == meta_size) break;
      if (errno == EINTR) continue;
      LOG(WARNING) << "failed to send message to node [" << id
                   << "] errno: " << errno << " " << zmq_strerror(errno);
      return -1;
    }
    zmq_msg_close(&meta_msg);
    int send_bytes = meta_size;

    // send data
    for (int i = 0; i < n; ++i) {
      zmq_msg_t data_msg;
      SArray<char>* data = new SArray<char>(msg.data[i]);
      int data_size = data->size();
      zmq_msg_init_data(&data_msg, data->data(), data->size(), FreeData, data);
      if (i == n - 1) tag = 0;
      while (true) {
        if (zmq_msg_send(&data_msg, socket, tag) == data_size) break;
        if (errno == EINTR) continue;
        LOG(WARNING) << "failed to send message to node [" << id
                     << "] errno: " << errno << " " << zmq_strerror(errno)
                     << ". " << i << "/" << n;
                     
        return -1;
      }
      zmq_msg_close(&data_msg);
      send_bytes += data_size;
    }
    return send_bytes;
  #else 
    return 0;
    // TODO ibv_post_send
  #endif
  }

  int RecvMsg(Message* msg) override {
    msg->data.clear();
    size_t recv_bytes = 0;
  #if USE_RDMA == 0
    for (int i = 0; ; ++i) {
      zmq_msg_t* zmsg = new zmq_msg_t;
      CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
      while (true) {
        if (zmq_msg_recv(zmsg, receiver_, 0) != -1) break;
        if (errno == EINTR) continue;
        LOG(WARNING) << "failed to receive message. errno: "
                     << errno << " " << zmq_strerror(errno);
        return -1;
      }
      char* buf = CHECK_NOTNULL((char *)zmq_msg_data(zmsg));
      size_t size = zmq_msg_size(zmsg);
      recv_bytes += size;

      if (i == 0) {
        // identify
        msg->meta.sender = GetNodeID(buf, size);
        msg->meta.recver = my_node_.id;
        CHECK(zmq_msg_more(zmsg));
        zmq_msg_close(zmsg);
        delete zmsg;
      } else if (i == 1) {
        // task
        UnpackMeta(buf, size, &(msg->meta));
        zmq_msg_close(zmsg);
        bool more = zmq_msg_more(zmsg);
        delete zmsg;
        if (!more) break;
      } else {
        // zero-copy
        SArray<char> data;
        data.reset(buf, size, [zmsg, size](char* buf) {
            zmq_msg_close(zmsg);
            delete zmsg;
          });
        msg->data.push_back(data);
        if (!zmq_msg_more(zmsg)) { break; }
      }
    }
  #else 
    // TODO ibv_post_recv
  #endif
    return recv_bytes;
  }

 private:
  /*
   * Counter generator for providing random port number 
   */
  static int ID() {
    static int ID = 0;
    return ID ++;
  }
  /**
   * return the node id given the received identity
   * \return -1 if not find
   */
  int GetNodeID(const char* buf, size_t size) {
    if (size > 2 && buf[0] == 'p' && buf[1] == 's') {
      int id = 0;
      size_t i = 2;
      for (; i < size; ++i) {
        if (buf[i] >= '0' && buf[i] <= '9') {
          id = id * 10 + buf[i] - '0';
        } else {
          break;
        }
      }
      if (i == size) return id;
    }
    return Meta::kEmpty;
  }

  void *context_ = nullptr;
  /**
   * \brief node_id to the socket for sending data to this node
   */
  std::unordered_map<int, void*> senders_;
  std::mutex mu_;
  void *receiver_ = nullptr;
};
}  // namespace ps

#endif  // PS_ZMQ_VAN_H_





// monitors the liveness other nodes if this is
// a schedule node, or monitors the liveness of the scheduler otherwise
// aliveness monitor
// CHECK(!zmq_socket_monitor(
//     senders_[kScheduler], "inproc://monitor", ZMQ_EVENT_ALL));
// monitor_thread_ = std::unique_ptr<std::thread>(
//     new std::thread(&Van::Monitoring, this));
// monitor_thread_->detach();

// void Van::Monitoring() {
//   void *s = CHECK_NOTNULL(zmq_socket(context_, ZMQ_PAIR));
//   CHECK(!zmq_connect(s, "inproc://monitor"));
//   while (true) {
//     //  First frame in message contains event number and value
//     zmq_msg_t msg;
//     zmq_msg_init(&msg);
//     if (zmq_msg_recv(&msg, s, 0) == -1) {
//       if (errno == EINTR) continue;
//       break;
//     }
//     uint8_t *data = static_cast<uint8_t*>(zmq_msg_data(&msg));
//     int event = *reinterpret_cast<uint16_t*>(data);
//     // int value = *(uint32_t *)(data + 2);

//     // Second frame in message contains event address. it's just the router's
//     // address. no help

//     if (event == ZMQ_EVENT_DISCONNECTED) {
//       if (!is_scheduler_) {
//         PS_VLOG(1) << my_node_.ShortDebugString() << ": scheduler is dead. exit.";
//         exit(-1);
//       }
//     }
//     if (event == ZMQ_EVENT_MONITOR_STOPPED) {
//       break;
//     }
//   }
//   zmq_close(s);
// }
