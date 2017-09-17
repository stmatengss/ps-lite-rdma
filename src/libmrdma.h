/*
 * Copyright @ stmatengss
 * email -> stmatengss@163.com
 * */
#ifndef LIBMRDMA_H

#define LIBMRDMA_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <time.h>

#define M_RC 0x0
#define M_UC 0x1
#define M_UD 0x2

struct m_param {
		int lid;
		int qpn;
		int psn;
};

struct m_priv_data {
		uint64_t buffer_addr;
		uint32_t buffer_rkey;
		size_t buffer_length;
};

struct m_ibv_res {

		int is_server;
		// int is_inline;
		struct ibv_ah *ah;
		struct ibv_device_attr *device_attr;
		struct ibv_port_attr *port_attr;
		struct m_priv_data *lpriv_data, *rpriv_data;
		struct m_param *lparam, *rparam;
		struct ibv_context *ctx;
		struct ibv_pd *pd;
		struct ibv_qp *qp;
		struct ibv_mr *mr;
		struct ibv_cq *send_cq, *recv_cq;
//		char *send_buffer;
//		char *recv_buffer;
		int model;
		int port;
		int ib_port;
		int sock;
		uint32_t qkey;
};

#define CPEA(ret) if (!ret) { \
		PRINT_LINE \
		printf("ERROR: %s\n", strerror(errno)); \
		printf("ERROR: NULL\n"); \
		exit(1); \
} 

#define CPEN(ret) if (ret == NULL) { \
		PRINT_LINE \
		printf("ERROR: %s\n", strerror(errno)); \
		printf("ERROR: NULL\n"); \
		exit(1); \
} 

#define CPE(ret) if (ret) { \
		PRINT_LINE \
		printf("ERROR: %s\n", strerror(errno)); \
		printf("ERROR CODE: %d\n", ret); \
		exit(ret); \
} 

#define FILL(st) do { \
		memset(&st, 0, sizeof(st)); \
} while(0);

#define time_sec (time((time_t *)NULL))
#define PRINT_TIME printf("time: %Lf\n", (long double)clock());
#define PRINT_LINE printf("line: %d\n", __LINE__);
#define PRINT_FUNC printf("func: %s\n", __FUNC__);

static void
m_nano_sleep(int nsec) {
		struct timespec tim, tim2;
		tim.tv_sec = 0;
		tim.tv_nsec = nsec;
		if (nanosleep(&tim, &tim2) < 0) {
				printf("SLEEP ERROR!\n");
				exit(1);
		}
}

static long long
m_get_usec() {
		struct timespec now;
		clock_gettime(CLOCK_REALTIME, &now);
		return (long long)now.tv_sec * 1000000 + now.tv_nsec / 1000;
}

static void 
m_client_exchange(const char *server, uint16_t port, struct m_param *lparam, 
				struct m_priv_data *lpriv_data, struct m_param **rparam,
				struct m_priv_data **rpriv_data) {

		int s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (s == -1) {

				printf("SOCK ERROR!\n");
				exit(1);
		}

		struct hostent *hent = gethostbyname(server);
		CPEN(hent);

		ssize_t tmp; // None-sense

		struct sockaddr_in sin;
		FILL(sin);
		sin.sin_family = PF_INET;
		sin.sin_port = htons(port);
		sin.sin_addr = *((struct in_addr *)hent->h_addr);

		m_nano_sleep(500000000);
		CPE((connect(s, (struct sockaddr *)&sin, sizeof(sin)) == -1));

//		PRINT_LINE

		tmp = write(s, lparam, sizeof(*lparam));
		tmp = read(s, *rparam, sizeof(*lparam));
		printf("remote lid %d, qpn %d\n", (*rparam)->lid, (*rparam)->qpn);

//		PRINT_LINE

		tmp = write(s, lpriv_data, sizeof(*lpriv_data));
		tmp = read(s, *rpriv_data, sizeof(*lpriv_data));
		printf("remote addr %ld, rkey %d\n", (*rpriv_data)->buffer_addr, 
						(*rpriv_data)->buffer_rkey);

		close(s);

}

static void 
m_server_exchange(uint16_t port, struct m_param *lparam, struct m_priv_data *lpriv_data,
				struct m_param **rparam, struct m_priv_data **rpriv_data) {

		int s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (s == -1) {

				printf("SOCK ERROR!\n");
				exit(1);
		}

		int on = 1;
		ssize_t tmp; // None-sense

		CPE((setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on) == -1));

		struct sockaddr_in sin;
		FILL(sin);
		sin.sin_family = PF_INET;
		sin.sin_port = htons(port);
		sin.sin_addr.s_addr = htons(INADDR_ANY); 

		CPE((bind(s, (struct sockaddr *)&sin, sizeof(sin)) == -1));

		CPE((listen(s, 1) == -1));

		struct sockaddr_in csin;
		socklen_t csinsize = sizeof(csin);

		int c =accept(s, (struct sockaddr *)&csin, &csinsize) ;
		CPE((c == -1));

		tmp = write(c, lparam, sizeof(*lparam));
		tmp = read(c, *rparam, sizeof(*lparam));

		printf("remote lid %d, qpn %d\n", lparam->lid, lparam->qpn);

		tmp = write(c, lpriv_data, sizeof(*lpriv_data));
		tmp = read(c, *rpriv_data, sizeof(*lpriv_data));

		printf("remote addr %ld, rkey %d\n", (*rpriv_data)->buffer_addr, 
						(*rpriv_data)->buffer_rkey);

		close(c);
		close(s);
}

static struct ibv_device *
m_get_deveice (int index) { 
		struct ibv_device **devs;
		int num;
		devs = ibv_get_device_list(&num);
		if (index >= num) {
				printf("Cannot get such device\n");
				return NULL;
		}
		return devs[index];
}

static int
m_get_lid (struct m_ibv_res *ibv_res) {
		struct ibv_port_attr port_attr;
		CPEN(ibv_res->ctx);
		printf("IB Port %d\n", ibv_res->ib_port);
		CPE(ibv_query_port(ibv_res->ctx, ibv_res->ib_port, &port_attr));
		printf("Get LID %x\n", port_attr.lid);
		return port_attr.lid;
}

static int
m_open_device_and_alloc_pd(struct m_ibv_res *ibv_res) {
		struct ibv_device *dev = m_get_deveice(0);	
		ibv_res->ctx = ibv_open_device(dev);
		CPEN(ibv_res->ctx);
		ibv_res->pd = ibv_alloc_pd(ibv_res->ctx);
		CPEN(ibv_res->pd);	
		return 0;
}

/*
 * TODO
 * */
static void 
m_reg_buffer(struct m_ibv_res *ibv_res, char *buffer, int size) {
		int flags = IBV_ACCESS_LOCAL_WRITE |
					IBV_ACCESS_REMOTE_WRITE |
					IBV_ACCESS_REMOTE_READ | 
					IBV_ACCESS_REMOTE_ATOMIC;
		if (ibv_res->mr == NULL) {
				ibv_res->mr = ibv_reg_mr(ibv_res->pd, buffer, size, flags);
				CPEN(ibv_res->mr);
		} else {
				printf("Already register\n");
				exit(1);
		}			
}


static void 
m_init_qp (struct m_ibv_res *ibv_res) {
		struct ibv_qp_attr qp_attr;
		FILL(qp_attr);
		qp_attr.qp_state = IBV_QPS_INIT;
		qp_attr.port_num = ibv_res->ib_port;
		qp_attr.pkey_index = 0;
		if (ibv_res->model == M_UD) {
				qp_attr.qkey = ibv_res->qkey; 
		} else {
				qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | 
				                  IBV_ACCESS_REMOTE_WRITE |
								  IBV_ACCESS_REMOTE_READ |
								  IBV_ACCESS_REMOTE_ATOMIC;
		}
		int flags = IBV_QP_STATE |
					IBV_QP_PKEY_INDEX |
					IBV_QP_PORT;
		if (ibv_res->model == M_UD) {
				flags |= IBV_QP_QKEY;
		} else {
				flags |= IBV_QP_ACCESS_FLAGS;
		}
		CPE(ibv_modify_qp(ibv_res->qp, &qp_attr, flags));
		printf("QPNum = %d\n", ibv_res->qp->qp_num);
}

static void
m_create_cq_and_qp (struct m_ibv_res *ibv_res, int max_dep, enum ibv_qp_type qp_type) {
		ibv_res->send_cq = ibv_create_cq(ibv_res->ctx, max_dep, NULL, NULL, 0);
		CPEN(ibv_res->send_cq);
		ibv_res->recv_cq = ibv_create_cq(ibv_res->ctx, max_dep, NULL, NULL, 0);
		CPEN(ibv_res->recv_cq);

		struct ibv_qp_init_attr qp_init_attr;
		FILL(qp_init_attr);
		qp_init_attr.send_cq = ibv_res->send_cq;
		qp_init_attr.recv_cq = ibv_res->recv_cq;
		qp_init_attr.cap.max_send_wr = max_dep;
		qp_init_attr.cap.max_recv_wr = max_dep;
		qp_init_attr.cap.max_send_sge = 1;
		qp_init_attr.cap.max_recv_sge = 1;
		qp_init_attr.cap.max_inline_data = 64; //TODO add support of inline
		qp_init_attr.qp_type = qp_type;
		qp_init_attr.sq_sig_all = 0; // TODO to complete the sig function
		
		ibv_res->qp = ibv_create_qp(ibv_res->pd, &qp_init_attr);
		CPEN(ibv_res->qp);

		m_init_qp(ibv_res);

}

static void
m_init_ah(struct m_ibv_res *ibv_res) {
		struct ibv_ah_attr ah_attr;
		ah_attr.dlid = ibv_res->rparam->lid;
		ah_attr.src_path_bits = 0;
		ah_attr.is_global = 0;
		ah_attr.sl = 0;
		ah_attr.port_num = ibv_res->ib_port;
		ibv_res->ah = ibv_create_ah(ibv_res->pd, &ah_attr);
}
/*
 * TODO
 * */
static void 
m_modify_qp_to_rts_and_rtr(struct m_ibv_res *ibv_res) {
		struct ibv_qp_attr qp_attr;
		FILL(qp_attr);
		int flags;
		qp_attr.qp_state = IBV_QPS_RTR;
		if (ibv_res->model == M_UD) {
				flags = IBV_QP_STATE;
		} else {
				flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
							IBV_QP_DEST_QPN | IBV_QP_RQ_PSN; 
				qp_attr.path_mtu = IBV_MTU_1024;
				qp_attr.dest_qp_num = ibv_res->rparam->qpn; 
				qp_attr.rq_psn = ibv_res->rparam->psn;
				if (ibv_res->model == M_RC) {
						qp_attr.max_dest_rd_atomic = 1;
						qp_attr.min_rnr_timer = 12;
						flags |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
				}
				qp_attr.ah_attr.is_global = 0;
				qp_attr.ah_attr.dlid = ibv_res->rparam->lid;
				qp_attr.ah_attr.sl = 0;
				qp_attr.ah_attr.src_path_bits = 0;
				qp_attr.ah_attr.port_num = ibv_res->ib_port;
		}	

		CPE(ibv_modify_qp(ibv_res->qp, &qp_attr, flags));
		
		qp_attr.qp_state = IBV_QPS_RTS;
		flags = IBV_QP_STATE | IBV_QP_SQ_PSN; 
		if (ibv_res->model == M_UD) {
				qp_attr.sq_psn = lrand48() & 0xffffff;		
		} else {
				qp_attr.sq_psn = ibv_res->lparam->psn;
				if (ibv_res->model == M_RC) {

						qp_attr.timeout = 14;
						qp_attr.retry_cnt = 7;
						qp_attr.rnr_retry = 7;
						qp_attr.max_rd_atomic = 1; //1 before
						flags |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | 
								IBV_QP_MAX_QP_RD_ATOMIC;
				}
				
		}		
		CPE(ibv_modify_qp(ibv_res->qp, &qp_attr, flags));
		if (ibv_res->model == M_UD ) {
				m_init_ah(ibv_res);
		}
}

/*
 * TODO
 * */
static void 
m_sync(struct m_ibv_res *ibv_res, const char *server, char *buffer) {
		ibv_res->lparam = (struct m_param *)malloc(sizeof(struct m_param));
		ibv_res->lparam->psn = lrand48() & 0xffffff;
		ibv_res->lparam->qpn = ibv_res->qp->qp_num;

		ibv_res->lparam->lid = m_get_lid(ibv_res);

		ibv_res->lpriv_data = (struct m_priv_data *)malloc(sizeof(struct m_priv_data));
		ibv_res->lpriv_data->buffer_addr = (uint64_t)buffer;
		ibv_res->lpriv_data->buffer_rkey = ibv_res->mr->rkey;
		ibv_res->lpriv_data->buffer_length = ibv_res->mr->length;

		ibv_res->rparam = (struct m_param *)malloc(sizeof(struct m_param));
		ibv_res->rpriv_data = (struct m_priv_data *)malloc(sizeof(struct m_priv_data));

		printf("Local LID = %d, QPN = %d, PSN = %d\n", 
						ibv_res->lparam->lid, ibv_res->lparam->qpn, ibv_res->lparam->psn);
		printf("Local Addr = %ld, RKey = %d, LEN = %zu\n",
						ibv_res->lpriv_data->buffer_addr, ibv_res->lpriv_data->buffer_rkey,
					   	ibv_res->lpriv_data->buffer_length);

		struct m_param param;
		if (ibv_res->is_server) {
//				ibv_res->rparam = m_server_exchange(ibv_res->port, ibv_res->lparam);
				m_server_exchange(ibv_res->port, ibv_res->lparam, ibv_res->lpriv_data,
								&ibv_res->rparam, &ibv_res->rpriv_data);
		} else {
				m_client_exchange(server, ibv_res->port, ibv_res->lparam, ibv_res->lpriv_data,
								&ibv_res->rparam, &ibv_res->rpriv_data);
//				ibv_res->rparam = m_client_exchange(server, ibv_res->port, ibv_res->lparam);
		}

		printf("Remote LID = %d, QPN = %d, PSN = %d\n", 
						ibv_res->rparam->lid, ibv_res->rparam->qpn, ibv_res->rparam->psn);

		printf("Local Addr = %ld, RKey = %d, LEN = %zu\n",
						ibv_res->rpriv_data->buffer_addr, ibv_res->rpriv_data->buffer_rkey,
					   	ibv_res->rpriv_data->buffer_length);
		
}

static inline void
m_post_send (struct m_ibv_res *ibv_res, char *buffer, size_t size) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_SEND;
		send_wr.send_flags = IBV_SEND_SIGNALED; // FIXME fuck

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_send_imm (struct m_ibv_res *ibv_res, char *buffer, size_t size, uint32_t imm_data) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.imm_data = imm_data;
		send_wr.opcode = IBV_WR_SEND_WITH_IMM;
		send_wr.send_flags = IBV_SEND_SIGNALED; // FIXME fuck

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_send_wrid (struct m_ibv_res *ibv_res, char *buffer, size_t size, int wr_id) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = wr_id;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_SEND;
		send_wr.send_flags = IBV_SEND_SIGNALED; // FIXME fuck

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_send_unsig (struct m_ibv_res *ibv_res, char *buffer, size_t size) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_SEND;

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_write_offset_sig (struct m_ibv_res *ibv_res, char *buffer, size_t size, int offset) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_RDMA_WRITE;
		send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data->buffer_addr + offset;
		send_wr.wr.rdma.rkey = ibv_res->rpriv_data->buffer_rkey;	
		send_wr.send_flags = IBV_SEND_SIGNALED; 

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_write_offset_sig_inline (struct m_ibv_res *ibv_res, char *buffer, size_t size, int offset) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_RDMA_WRITE;
		send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data->buffer_addr + offset;
		send_wr.wr.rdma.rkey = ibv_res->rpriv_data->buffer_rkey;	
		send_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE; 

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_read_offset_sig (struct m_ibv_res *ibv_res, char *buffer, size_t size, int offset) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_RDMA_READ;
		send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data->buffer_addr + offset;
		send_wr.wr.rdma.rkey = ibv_res->rpriv_data->buffer_rkey;
		send_wr.send_flags = IBV_SEND_SIGNALED; 

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_write_offset (struct m_ibv_res *ibv_res, char *buffer, size_t size, int offset) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_RDMA_WRITE;
		send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data->buffer_addr + offset;
		send_wr.wr.rdma.rkey = ibv_res->rpriv_data->buffer_rkey;	

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_write_offset_inline (struct m_ibv_res *ibv_res, char *buffer, size_t size, int offset) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_RDMA_WRITE;
		send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data->buffer_addr + offset;
		send_wr.wr.rdma.rkey = ibv_res->rpriv_data->buffer_rkey;	
		send_wr.send_flags = IBV_SEND_INLINE; 

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_read_offset (struct m_ibv_res *ibv_res, char *buffer, size_t size, int offset) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_RDMA_READ;
		send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data->buffer_addr + offset;
		send_wr.wr.rdma.rkey = ibv_res->rpriv_data->buffer_rkey;
//		send_wr.send_flags = IBV_SEND_SIGNALED; // FIXME fuck

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_write (struct m_ibv_res *ibv_res, char *buffer, size_t size) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_RDMA_WRITE;
		send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data->buffer_addr;
		send_wr.wr.rdma.rkey = ibv_res->rpriv_data->buffer_rkey;	

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_cas(struct m_ibv_res *ibv_res, char *buffer, uint64_t compare_add, uint64_t swap) {
		struct ibv_sge sge = {
				(uint64_t)buffer, 8, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;

		send_wr.wr.atomic.remote_addr = ibv_res->rpriv_data->buffer_addr;
		send_wr.wr.atomic.rkey = ibv_res->rpriv_data->buffer_rkey;
		send_wr.wr.atomic.compare_add = compare_add;
		send_wr.wr.atomic.swap = swap;

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_faa(struct m_ibv_res *ibv_res, char *buffer, uint64_t compare_add) {
		struct ibv_sge sge = {
				(uint64_t)buffer, 8, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;

		send_wr.wr.atomic.remote_addr = ibv_res->rpriv_data->buffer_addr;
		send_wr.wr.atomic.rkey = ibv_res->rpriv_data->buffer_rkey;
		send_wr.wr.atomic.compare_add = compare_add;

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_faa_offset(struct m_ibv_res *ibv_res, char *buffer, uint64_t compare_add, int offset) {
		struct ibv_sge sge = {
				(uint64_t)buffer, 8, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;

		send_wr.wr.atomic.remote_addr = ibv_res->rpriv_data->buffer_addr + offset;
		send_wr.wr.atomic.rkey = ibv_res->rpriv_data->buffer_rkey;
		send_wr.wr.atomic.compare_add = compare_add;

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_read (struct m_ibv_res *ibv_res, char *buffer, size_t size) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr_id = 2;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_RDMA_READ;
		send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data->buffer_addr;
		send_wr.wr.rdma.rkey = ibv_res->rpriv_data->buffer_rkey;
//		send_wr.send_flags = IBV_SEND_SIGNALED; // FIXME fuck

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void
m_post_ud_send (struct m_ibv_res *ibv_res, char *buffer, size_t size) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_send_wr send_wr;
		FILL(send_wr);
		send_wr.wr.ud.ah = ibv_res->ah;
		send_wr.wr.ud.remote_qpn = ibv_res->rparam->qpn; 
		send_wr.wr.ud.remote_qkey = ibv_res->qkey;
		send_wr.wr_id = 0;
		send_wr.next = NULL;
		send_wr.sg_list = &sge;
		send_wr.num_sge = 1;
		send_wr.opcode = IBV_WR_SEND;
		send_wr.send_flags = IBV_SEND_SIGNALED;

		struct ibv_send_wr *bad_send_wr;
		CPE(ibv_post_send(ibv_res->qp, &send_wr, &bad_send_wr));
}

static inline void 
m_post_recv (struct m_ibv_res *ibv_res, char *buffer, size_t size) {
		struct ibv_sge sge = {
				(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_recv_wr recv_wr;
		FILL(recv_wr);
		recv_wr.wr_id = 0;
		recv_wr.next = NULL;
		recv_wr.sg_list = &sge;
		recv_wr.num_sge = 1;
		
		struct ibv_recv_wr *bad_recv_wr;
		CPE(ibv_post_recv(ibv_res->qp, &recv_wr, &bad_recv_wr));
}

static inline void
m_poll_send_cq(struct m_ibv_res *ibv_res) {
		struct ibv_wc wc;
		while(ibv_poll_cq(ibv_res->send_cq, 1, &wc) < 1);
		if (wc.status != IBV_WC_SUCCESS) {
				printf("Status: %d\n", wc.status);
				printf("Ibv_poll_cq error!\n");
				printf("Error: %s\n", strerror(errno));
				exit(1);
		}
}

static inline void
m_poll_recv_cq(struct m_ibv_res *ibv_res) {
		struct ibv_wc wc;
		while(ibv_poll_cq(ibv_res->recv_cq, 1, &wc) < 1);
		if (wc.status != IBV_WC_SUCCESS) {
				printf("Ibv_poll_cq error!\n");
				printf("Error: %s\n", strerror(errno));
				exit(1);
		}
}	

static inline void
m_poll_recv_cq_multi(struct m_ibv_res *ibv_res, int number) {
		struct ibv_wc wc;
		while(number > 0) {
				int poll_num = ibv_poll_cq(ibv_res->recv_cq, number, &wc);
				if (poll_num != 0) {
						number -= poll_num;
						if (wc.status != IBV_WC_SUCCESS) {
								printf("Ibv_poll_cq error!\n");
								printf("Error: %s\n", strerror(errno));
								exit(1);
						}
						printf("poll num: %d, number: %d\n", poll_num, number);
				}
		}
}	

static inline uint32_t
m_poll_recv_cq_with_data(struct m_ibv_res *ibv_res) {
		struct ibv_wc wc;
		while(ibv_poll_cq(ibv_res->recv_cq, 1, &wc) < 1);
		if (wc.status != IBV_WC_SUCCESS) {
				printf("Ibv_poll_cq error!\n");
				printf("Error: %s\n", strerror(errno));
				exit(1);
		}
		return wc.imm_data;
}

#endif
