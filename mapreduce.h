#include<sys/types.h>
#include<sys/socket.h>
#include<sys/ioctl.h>
#include<signal.h>
#include<unistd.h>
#include<fcntl.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<netinet/in.h>
#include<errno.h>
#include<netdb.h>
#include<time.h>
#include<dirent.h>
#include<math.h>

#include<vector>
#include<unordered_map>
#include<algorithm>
#include<thread>
#include<fstream>
#include<iostream>
#include<sstream>
#include<utility>

using namespace std;

#define MSGLEN   500
#define PORTBASE 20000

enum S_TYPE {
    MASTER=0,
    MAPPER=1,
    REDUCER=2
};

enum MSG_TYPE {
    READY=0,
    START=1,
    FINISH=2,
    CLOSE=3
};

typedef struct {
    enum MSG_TYPE message_type;
    enum S_TYPE server_type;
	int msg_len;
    // int server_id;
	int origin;
	int from;
	int seq_num;
	// uint16_t payload[MSGLEN];
    char msg[MSGLEN];
} __attribute__((packed)) message;

typedef struct state_t {
    enum S_TYPE server_type;
	int sockfd;
	int udp_sockfd;
	int port;

	int server_id; // include all server
    int filepos;
    int num_reducers;
    int num_workers;
    int finished_reducer;
    string inputdir;
    string outputdir;

	struct sockaddr_in udp_socket;
	struct sockaddr_in master;
	socklen_t socklen;

    int finished_map_task;
	int num_servers;
	// message status;
	// int status[PEERNUM];
} state_t;


void master_init();
void server_init(int server_id);
void read_filename(const char *dirname);

void setup_udpsocket(int server_id);
void setup_reducer();

void do_map(string filename);
void do_reduce();
void send_to_mapper(int server_id);
void send_to_reducer(int server_id);
void send_to_master(enum MSG_TYPE message_type);
void server_recv();

int guard(int n, char *err) { if (n == -1) { perror(err); exit(-1); } return n; }