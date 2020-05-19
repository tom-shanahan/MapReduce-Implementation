#include "mapreduce.h"

state_t s;
int sort_flag;

DIR *dir;
struct dirent *ent;

vector<string> filenames;

void alarm_handler(int sig) {
    signal(SIGALRM, SIG_IGN);
	printf("timeout, closing socket");
	close(s.sockfd);

	exit(0);
    signal(SIGALRM, alarm_handler);
    // alarm(TIMEOUT);

    // alarm(5);
}

void master_init(){
    setup_udpsocket(0);
    s.filepos = 0;
    s.finished_reducer = 0;
    s.finished_map_task = 0;

    s.master = s.udp_socket;
    s.server_type = MASTER;
}

void server_init(int server_id){
    s.server_id = server_id;
    printf("server_init(): setting up server%d\n", server_id);
    setup_udpsocket(server_id); 
}

void read_filename(const char *dirname){
    if ((dir = opendir (dirname)) != NULL) {
        /* print all the files and directories within directory */
        printf ("read_filename(): filename read...\n");
        while ((ent = readdir (dir)) != NULL) {
            if(strncmp(ent -> d_name, ".", 1) != 0 && strncmp(ent -> d_name, "..", 2) != 0){
                filenames.push_back(ent -> d_name);
                printf ("%s\n", ent -> d_name);
            }       
        }
        closedir (dir);
    } else {
        /* could not open directory */
        perror ("read_filename(): ");
    }
}

void setup_udpsocket(int server_id) {
	s.udp_sockfd = guard(socket(AF_INET, SOCK_DGRAM, 0),
		"setup_udpsocket(): error in socket().");

	memset(&s.udp_socket, 0, sizeof(struct sockaddr_in));
	s.udp_socket.sin_family      = AF_INET;
	s.udp_socket.sin_addr.s_addr = htonl(INADDR_ANY);
	s.udp_socket.sin_port        = htons(PORTBASE + server_id);

	int option = 1;
	guard(setsockopt(s.udp_sockfd, SOL_SOCKET, SO_REUSEADDR, &option,
		sizeof(option)), "setup_udpsocket(): error in setsockopt().");
	guard(fcntl(s.udp_sockfd, F_SETFL, fcntl(s.udp_sockfd, F_GETFL, 0) | O_NONBLOCK),
		"setup_udpsocket(): error in fcntl()");
	guard(bind(s.udp_sockfd, (struct sockaddr *)&s.udp_socket,
		sizeof(struct sockaddr_in)), "setup_udpsocket(): error in bind().");
	printf("server_type %d server%d setup_udpsocket(): success.\n", s.server_type, server_id);
	fflush(stdout);
}

void setup_reducer(){
    // start reducer
    printf("begin setup_reducer()\n");
    int cpid;
    for(int i = 1; i <= s.num_reducers; i++){
        if((cpid = fork()) == 0){
            printf("setup_reducer: %d\n", i);
            s.server_type = REDUCER;
            server_init(s.num_workers + i);
            send_to_master(READY);
            server_recv();
            exit(0);
        }
    }
}



void do_map(string filename){
    ifstream file(s.inputdir + "/" + filename);
    string line;
    ofstream outfiles[s.num_reducers];
    // ofstream outfile("output.txt", std::ios_base::app);  

    for(int i = 0; i < s.num_reducers; i++){
        string name = s.outputdir;
        name += string("/map.part-") + to_string(s.server_id-1) + string("-") + to_string(i) + string(".txt");
        cout << name << endl;
        outfiles[i].open(name, std::ios_base::app);
    }

    while(getline(file, line)){
        // string buf;
        // stringstream ss(line);
        
        // while(ss >> buf){
        //     outfile << buf << ",1" << endl;
        // }
        
        for (int i = 0; i < line.length(); i++)
        {
            string word = "";
            if(line[i] != ' '){
                while(i < line.length() && line[i] != ' '){
                    if((line[i] >= 'a' && line[i] <= 'z') || (line[i] >='A' && line[i] <= 'Z')){
                        word += tolower(line[i]);
                    } 
                    i++;
                }

                if(word.length() != 0){
                    // hash word to different files
                    int index = hash<string>()(word) % s.num_reducers;
                    outfiles[index] << word << ",1" << endl;
                }  
            }   
        }
    }
    printf("do_map(): mapper%d done map\n", s.server_id-1);
    fflush(stdout);
}

void do_sort_map(string filename){
    ifstream file(s.inputdir + "/" + filename);
    string line;
    ofstream outfiles[s.num_reducers];
    // ofstream outfile("output.txt", std::ios_base::app);

    for(int i = 0; i < s.num_reducers; i++){
        string name = s.outputdir;
        name += string("/map.part-") + to_string(s.server_id-1) + string("-") + to_string(i) + string(".txt");
        cout << name << endl;
        outfiles[i].open(name, std::ios_base::app);
    }

    while(getline(file, line)){
        for (int i = 0; i < line.length(); i++)
        {
            string word = "";
            if(line[i] != ' '){
                while(i < line.length() && line[i] != ' '){
                    word += tolower(line[i]);
                    i++;
                }

                if(word.length() != 0){
                    // hash number to file based on where they appear in the distribution
                    int number = stoi(word);
                    int bucket_size = ceil((pow(2,30)-1) / s.num_reducers);
                    div_t output = div(number, bucket_size);
                    int index = output.quot;
                    outfiles[index] << number << endl;
                }
            }
        }
    }
    printf("do_sort_map(): mapper%d done map\n", s.server_id-1);
    fflush(stdout);
}

void do_reduce(){
    // do reducer job
    int reducer_id = s.server_id - s.num_workers - 1;
    read_filename(s.outputdir.c_str());
    string outputname = s.outputdir + string("/reduce.part-") + to_string(reducer_id) + string(".txt");
    ofstream outfile(outputname, std::ios_base::app);

    unordered_map<string, int> umap; 

    for(int i = 0; i < s.num_workers; i++){
        string name = string("map.part-") + to_string(i) + string("-") + to_string(reducer_id) + string(".txt");

        // if file exists
        if(find(filenames.begin(), filenames.end(), name) != filenames.end()){
            ifstream file(s.outputdir + "/" + name);
            string line;

            while(getline(file, line)){
                for (int i = 0; i < line.length(); i++)
                {
                    string word = "";
                    if(line[i] >= 'a' && line[i] <= 'z'){
                        while(i < line.length() && (line[i] >= 'a' && line[i] <= 'z')){
                            word += line[i];
                            i++;
                        }
                        umap[word]++;
                    }   
                }
            }
        }
    }

    //write to output file
    for(auto x : umap){
        outfile << x.first << "," << x.second << endl;
    }
    printf("do_reduce(): reducer%d done reduce\n", reducer_id);
    fflush(stdout);

    send_to_master(FINISH);
    exit(0);
}

void do_sort_reduce(){
    int reducer_id = s.server_id - s.num_workers - 1;
    read_filename(s.outputdir.c_str());
    string outputname = s.outputdir + string("/reduce.part-") + to_string(reducer_id) + string(".txt");
    ofstream outfile(outputname, std::ios_base::app);

    vector<int> number_map;
    int number;

    for(int i = 0; i < s.num_workers; i++){
        string name = string("map.part-") + to_string(i) + string("-") + to_string(reducer_id) + string(".txt");

        // if file exists
        if(find(filenames.begin(), filenames.end(), name) != filenames.end()){
            ifstream file(s.outputdir + "/" + name);
            string line;

            while(getline(file, line)){
                for (int i = 0; i < line.length(); i++)
                {
                    string word = "";
                    while(i < line.length()) {
                        word += line[i];
                        i++;
                    }
                    number = stoi(word);
                }
                number_map.push_back(number);
            }
        }
    }

    sort(number_map.begin(), number_map.end());

    //write to output file
    for(auto x : number_map){
        outfile << x << endl;
    }
    printf("do_reduce(): reducer%d done reduce\n", reducer_id);
    fflush(stdout);

    send_to_master(FINISH);
    exit(0);
}

void send_to_mapper(int server_id) {
	struct sockaddr_in dest = s.udp_socket;
    dest.sin_port = htons(PORTBASE + server_id);

	message msg;
    msg.server_type = s.server_type;
	msg.message_type = START;
	// status.server_id = s.server_id;
	msg.from = s.server_id;
	msg.origin = s.server_id;

    memset(msg.msg, 0, MSGLEN);
    strcpy(msg.msg, filenames[s.filepos].c_str());
    s.filepos++;
	
	printf("send_to_mapper(): send start to mapper %d, filename %s.\n", server_id-1, msg.msg);
	fflush(stdout);
	guard(sendto(s.udp_sockfd, &msg, sizeof(msg), 0,
		(struct sockaddr *)&dest, sizeof(dest)),
		"send_to_mapper(): error in sendto().");
}

void send_to_reducer(int server_id) {
	struct sockaddr_in dest = s.udp_socket;
    dest.sin_port = htons(PORTBASE + server_id);

	message msg;
    msg.server_type = s.server_type;
	msg.message_type = START;
	msg.from = s.server_id;
	msg.origin = s.server_id;

    // memset(msg.msg, 0, MSGLEN);
    // strcpy(msg.msg, filenames[s.filepos].c_str());
    // s.filepos++;
	
	printf("send_to_reducer(): send start to reducer %d.\n", server_id-s.num_workers);
	fflush(stdout);
	guard(sendto(s.udp_sockfd, &msg, sizeof(msg), 0,
		(struct sockaddr *)&dest, sizeof(dest)),
		"send_to_reducer(): error in sendto().");
}

void send_close(int server_id) {
	struct sockaddr_in dest = s.udp_socket;
    dest.sin_port = htons(PORTBASE + server_id);

	message msg;
    msg.server_type = s.server_type;
	msg.message_type = CLOSE;
	msg.from = s.server_id;
	msg.origin = s.server_id;

    // memset(msg.msg, 0, MSGLEN);
    // strcpy(msg.msg, filenames[s.filepos].c_str());
    // s.filepos++;
	
	printf("send_close(): send close to server %d.\n", server_id);
	fflush(stdout);
	guard(sendto(s.udp_sockfd, &msg, sizeof(msg), 0,
		(struct sockaddr *)&dest, sizeof(dest)),
		"send_close(): error in sendto().");
}

void send_to_master(enum MSG_TYPE message_type) {
	struct sockaddr_in dest = s.master;

	message msg;
    msg.server_type = s.server_type;
	msg.message_type = message_type;
	// status.server_id = s.server_id;
	msg.from = s.server_id;
	msg.origin = s.server_id;
	
    if(message_type == READY){
        printf("server%d send_to_master(): send ready to master.\n", s.server_id);
    }else if(message_type == FINISH){
        printf("server%d send_to_master(): send finish to master.\n", s.server_id);
    }
	
	fflush(stdout);
	guard(sendto(s.udp_sockfd, &msg, sizeof(msg), 0,
		(struct sockaddr *)&dest, sizeof(dest)),
		"send_to_master(): error in sendto().");
}


void do_merge(){
    // do merge job
    int reducer_id = s.server_id - s.num_workers - 1;
    read_filename(s.outputdir.c_str());
    string outputname = s.outputdir + string("/output.txt");
    ofstream outfile(outputname, std::ios_base::trunc);

    vector< pair<int, string> > merged_maps;

    for(int i = 0; i < s.num_reducers; i++){
        string name = string("reduce.part-") + to_string(i) + string(".txt");

        if(find(filenames.begin(), filenames.end(), name) != filenames.end()){
            ifstream file(s.outputdir + "/" + name);
            string line;

            while(getline(file, line)){
                int comma = 0;
                string word = "";
                string count = "";
                for (int i = 0; i < line.length(); i++)
                {
                    if(line[i] == ','){
                        comma = 1;
                        continue;
                    }
                    
                    if(comma == 0){
                        word += line[i];
                    }else if(comma == 1){
                        count += line[i];
                    }
                }
                int num;
                num = stoi(count);
                pair <int, string> word_pair (num, word);
                merged_maps.push_back(word_pair);
            }
        }
    }

    sort(merged_maps.begin(), merged_maps.end());
    reverse(merged_maps.begin(), merged_maps.end());

    //write to output file
    for(auto x : merged_maps){
        outfile << x.second << "," << x.first << endl;
    }
    printf("do_merge(): done\n");
    fflush(stdout);
}


void do_sort_merge(){
    int reducer_id = s.server_id - s.num_workers - 1;
    read_filename(s.outputdir.c_str());
    string outputname = s.outputdir + string("/output.txt");
    ofstream outfile(outputname, std::ios_base::trunc);

    vector<int> number_map;
    int number;

    for(int i = 0; i < s.num_reducers; i++){
        string name = string("reduce.part-") + to_string(i) + string(".txt");

        if(find(filenames.begin(), filenames.end(), name) != filenames.end()){
            ifstream file(s.outputdir + "/" + name);
            string line;

            while(getline(file, line)){
                int comma = 0;
                string word = "";
                for (int i = 0; i < line.length(); i++) {
                    word += line[i];
                }
                number = stoi(word);
                number_map.push_back(number);
            }
        }
    }

    //write to output file
    for(auto x : number_map){
        outfile << x << endl;
    }
    printf("do_merge(): done\n");
    fflush(stdout);
}



void server_recv() {
	fd_set sockfd_set;
	struct timeval timeout = {5, 0}; // timeout for select
	int smax = s.udp_sockfd;

	signal(SIGALRM, alarm_handler);
	alarm(120);
	while (1) {
	    FD_ZERO(&sockfd_set);
	    FD_SET(s.udp_sockfd, &sockfd_set);

	    int retval = select(smax + 1, &sockfd_set, NULL, NULL, &timeout);
	    if (retval > 0) { // sockets readable (incoming msgs)
			alarm(120); // reset timer

			if (FD_ISSET(s.udp_sockfd, &sockfd_set)) {
				message msg;
				int num_read = guard(read(s.udp_sockfd, (char *)&msg, sizeof(msg)),
					"server_recv(): error in udp_sockfd read().");
				if (num_read > 0) {
                    if(s.server_type == MASTER){
                        if(msg.message_type == READY){
                            if(msg.server_type == MAPPER){
                                printf("server_recv(): server mapper%d ready\n", msg.from-1);
                                if(s.filepos < filenames.size()){
                                    send_to_mapper(msg.from);
                                }
                            }else if(msg.server_type == REDUCER){
                                send_to_reducer(msg.from);
                            }
                        }else if(msg.message_type == FINISH){
                            if(msg.server_type == MAPPER){
                                printf("server_recv(): server mapper%d finish\n", msg.from-1);
                                s.finished_map_task++;
                                if(s.filepos < filenames.size()){
                                    send_to_mapper(msg.from);
                                }else{
                                    send_close(msg.from);

                                    // begin reduce
                                    if(s.finished_map_task == filenames.size()){
                                        setup_reducer();  
                                    }
                                       
                                }
                            }else if(msg.server_type == REDUCER){
                                printf("server_recv(): server reducer%d finish\n", msg.from-s.num_workers-1);
                                s.finished_reducer++;
                                if(s.finished_reducer == s.num_reducers){
                                    // do_merge
                                    if (sort_flag == 1){
                                        do_sort_merge();
                                    }else{
                                        do_merge();

                                    }
                                    printf("killall\n");
                                    string str = "killall mapreduce";
                                    const char *command = str.c_str();
                                    system(command);

                                    exit(0);
                                }
                            }   
                        }
                    }else if(s.server_type == MAPPER){
                        if(msg.message_type == START){
                            printf("server_recv(): server%d, receive START for %s\n", s.server_id, msg.msg);
                            if (sort_flag == 1){
                                do_sort_map(string(msg.msg));
                            }else {
                                do_map(string(msg.msg));
                            }
                            send_to_master(FINISH);
                        }else if(msg.message_type == CLOSE){
                            printf("server_recv(): server%d, receive CLOSE, close\n", s.server_id);
                            exit(0);
                        }
                        
                    }else if(s.server_type == REDUCER){
                        if(msg.message_type == START){
                            printf("server_recv(): reducer%d, receive START\n", s.server_id - s.num_workers);
                        }

                        if (sort_flag == 1){
                            do_sort_reduce();
                        }else{
                            do_reduce();
                        }
                    }
				}
	        }
	    } else if (retval == 0) { // timeout & receive nothing, send randomly
            printf("server_recv(): timeout, resent ready.\n");
			send_to_master(READY);
		} else if (retval < 0) {
			perror("server_recv(): error in select().");
			exit(1);
	    }
	}
}



int main(int argc, char *argv[]) {
    sort_flag = 0;

    for(int i = 1; i < argc; i++){
        if(string(argv[i]) == "--input"){
            //delete last slash
            s.inputdir = string(argv[++i]);
            s.inputdir.pop_back();
        }else if(string(argv[i]) == "--output"){
            //delete last slash
            s.outputdir = string(argv[++i]);
            s.outputdir.pop_back();
        }else if(string(argv[i]) == "--nworkers"){
            s.num_workers = atoi(argv[++i]);
        }else if(string(argv[i]) == "--nreduce"){
            s.num_reducers = atoi(argv[++i]);
        }else if(string(argv[i]) == "--sort"){
            sort_flag = 1;
        }
    }

    master_init();
    read_filename(s.inputdir.c_str());
	
    int cpid;
    for(int i = 1; i <= s.num_workers; i++){
        if((cpid = fork()) == 0){
            s.server_type = MAPPER;
            server_init(i);
            send_to_master(READY);
            server_recv();

            exit(0);
        }
    }

    // send_to_mapper(1);
    // send_to_mapper(2);
    // // send_to_mapper(3);

    server_recv();

    // do_map("test.txt");
    // do_map("test2.txt");
    
    
    // printf("end\n");
}