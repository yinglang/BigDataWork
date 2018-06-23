/**
 Created by ghn on 18-6-6.
 */

#include <stdio.h>
#include <map>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) SimRank##name
#define EPS 1e-6

/**
 * ALIVE: msg that random walking in graph.
 * DEAD: when msg can not be send out, it will send back to source vertex to update it value for output.
 * ALONE: msg tah random walking in graph, but it's source vertex has no similar calculation to other vertex.
 * */
enum MSGFLAG{ALIVE, DEAD};

// define the msg struct
typedef struct {
    uint64_t i;
    uint64_t j;
    double sim;
    MSGFLAG flag=MSGFLAG::ALIVE;
}msg_struct;

typedef struct {
    uint64_t source_id;
    map<uint64_t, double> sim;
    MSGFLAG flag;
}merge_msg_struct;

// define the vertex value struct
typedef struct {
    map<uint64_t, double>* sim = NULL;
}vertex_struct;

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(vertex_struct);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(msg_struct);
        return m_m_value_size;
    }
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;
        int outdegree = 0;

        const char *line= getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable

        sscanf(line, "%lld %lld", &from, &to);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line= getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable

            sscanf(line, "%lld %lld", &from, &to);
            if (last_vertex != from) {
                vertex_struct vertex_value;
                addVertex(last_vertex, &vertex_value, outdegree);
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        vertex_struct vertex_value;
        addVertex(last_vertex, &vertex_value, outdegree);
    }
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<double> {
public:
    void init() {
        m_global = 0;
        m_local = 0;
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        m_global = * (double *)p;
    }
    void* getLocal() {
        return &m_local;
    }
    void merge(const void* p) {
        m_global += * (double *)p;
    }
    void accumulate(const void* p) {
        m_local += * (double *)p;
    }
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    static unsigned long long k; // iterator k
    static unsigned long long l; // max path lenth l
    static string logpath;
    static double c; // the losing arg
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: PageRankVertex.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    void init(int argc, char* argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 6) {
           printf ("Usage: %s <input path> <output path> <iterator k> <max_pathLenth l> <arg c> [logpath]\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
        sscanf(argv[3], "%lld", &k);
        sscanf(argv[4], "%lld", &l);
        sscanf(argv[5], "%lf", &c);

        if(argc > 6) {
            char s[300];
            sscanf(argv[6], "%s", s);
            VERTEX_CLASS_NAME(Graph)::logpath = string(s);
        }

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
    }

    void term() {
        delete[] aggregator;
    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() override {
        int64_t vid;
        vertex_struct value;
        char s[1024];
        auto K=VERTEX_CLASS_NAME(Graph)::k;

        for (ResultIterator r_iter; ! r_iter.done(); r_iter.next() ) {
            r_iter.getIdValue(vid, &value);
            if(value.sim != NULL) {
                int n = sprintf(s, "%lld: {", (unsigned long long)vid);
                writeNextResLine(s, n);
                for(auto iter = value.sim->begin(); iter != value.sim->end(); iter++){
                    n = sprintf(s, " %lld: %lf,", (unsigned long long)iter->first, iter->second / K);
                    writeNextResLine(s, n);
                }
                n = sprintf(s, "}\n");
                writeNextResLine(s, n);
            }
        }
    }
};

// init the static var k,l
unsigned long long VERTEX_CLASS_NAME(Graph)::k = 0;
unsigned long long VERTEX_CLASS_NAME(Graph)::l = 0;
double VERTEX_CLASS_NAME(Graph)::c = 0;
string VERTEX_CLASS_NAME(Graph)::logpath = "";

class VERTEX_CLASS_NAME(): public Vertex <vertex_struct, double, msg_struct> {
public:

    void compute(MessageIterator* pmsgs) override {
        auto * msgs = new vector<merge_msg_struct*>();;
        // init in 0-st super step
        if(getSuperstep() == 0){
            mutableValue()->sim = new map<uint64_t , double>();
        }else {
            collect_merge_msg(pmsgs, msgs);
        }

        srand((unsigned)time(NULL));
        deal(*msgs);
    }

    void deal(const vector<merge_msg_struct*>& msgs){
//        ofstream cout;
//        cout.open("/home/hui/github/BigDataWork/bigwork/SimRank/log.txt", ios_base::app);

        vector<merge_msg_struct*> received_msg; // received msg
        // merge dead msg to value in (superStep %L == 0) cause all alive msg set to dead in superStep() % L == L-1
        // merge dead msg to value in (superStep %L > 0)  cause some msg sent to a zero out_degree vertex, the msg will dead.
        merge_dead_msg_and_get_alive_msg(msgs, received_msg);

        auto K=VERTEX_CLASS_NAME(Graph)::k, L = VERTEX_CLASS_NAME(Graph)::l;
        // Superstep == k*l, computing stops
        if (getSuperstep() == K * L){
            output_reslut();
            voteToHalt();
            return;
        }// Superstep % l == 0 means the start of a new loop
        else if (getSuperstep()% L == 0){
            output_reslut();   // for test

            if(getOutEdgeIterator().size() !=0) { // if size=0, do nothing
                auto * msg = new merge_msg_struct();
                msg->source_id = (uint64_t)getVertexId();
                // randomly send msg to one neighbor
                random_send_to_neighbour(msg);
            }
        }// these vertexs receive msg, update the map, and send msg
        else{
            // deal all ALIVE msg, cal meet msg similar
            cal_similar_of_alive_msgs(received_msg);

//            cout << "sp: "<< getSuperstep() << ", vid: " << getVertexId() << ", rmsg_size: "
//                 << received_msg.size() << ", vsize: " << getValue().sim->size() << endl;

            // send out, if have out neighbour, continue rand walk for all alive msg
            if((getSuperstep() % L < L - 1) && getOutEdgeIterator().size() > 0){ // if size=0, then stops to send msg
                for (const auto &i : received_msg) {
                    // randomly send msg to one neighbor
                    random_send_to_neighbour(i);
                }
            }
            // if the vertex has no neighbor, then write back all the received map of msg
            else{
                for (auto msg : received_msg) {
                    msg->flag = MSGFLAG::DEAD;
                    send_merge_msg(msg->source_id, *msg);
                }
            }
        }
    }

    // random choose a neighbour and send msg out to it.
    void random_send_to_neighbour(merge_msg_struct* msg){
        vector<int64_t> neighbors_id;
        for(auto iter=getOutEdgeIterator(); !iter.done(); iter.next()){
            neighbors_id.push_back(iter.target());
        }
        int64_t random_neighbor = rand() % (getOutEdgeIterator().size());
        send_merge_msg(neighbors_id.at(random_neighbor), *msg);

        //cout << "   vid: " << getVertexId() << ",ssize: " << i->sim.size() << ", neighbour: " << neighbors_id.at(random_neighbor)<< endl;
    }

    // cal similar of meet vertex msgs
    void cal_similar_of_alive_msgs(vector<merge_msg_struct*>& received_msg){
        int kk = 0;
        const double C_K = pow(VERTEX_CLASS_NAME(Graph)::c, getSuperstep()% VERTEX_CLASS_NAME(Graph)::l);
        for(uint64_t  i=0; i+1<received_msg.size(); i++) {
            for (uint64_t j = i + 1; j < received_msg.size(); j++) {
                auto msg_i = received_msg.at(i);
                auto msg_j = received_msg.at(j);
                if(msg_i->source_id > msg_j->source_id){
                    auto temp = msg_i;
                    msg_i = msg_j;
                    msg_j = temp;
                }

                // if the map of i_vertex doesn't have j_vertex, then insert j_vertex
                if(msg_i->sim.find(msg_j->source_id) == msg_i->sim.end()){
                    msg_i->sim.insert(pair<uint64_t, double>(msg_j->source_id, C_K));
                    kk++;
                }
            }
        }
    }

    // merge dead msg to value in (superStep %L == 0) cause all alive msg set to dead in superStep() % L == L-1
    // merge dead msg to value in (superStep %L > 0)  cause some msg sent to a zero out_degree vertex, the msg will dead.
    void merge_dead_msg_and_get_alive_msg(const vector<merge_msg_struct*>& msgs,
                                          vector<merge_msg_struct*>& alive_msg){
        for(auto& msg: msgs){
            //  solve these alive msg
            if(msg->flag != MSGFLAG::DEAD)
                alive_msg.push_back(msg);
            else{ // merge these dead msg
                merge_map(mutableValue()->sim, msg->sim);
            }
        }
    }

    // merge two map
    void merge_map(map<uint64_t, double>* map1, const map<uint64_t, double>& map2){
        for (const auto &it : map2) {
            if(map1->find(it.first) != map1->end()){ // map1 already has the key, then add
                (*map1)[it.first] += it.second;
            } // not exist, then insert
            else{
                (*map1)[it.first] = it.second;
            }
        }
    }

    void collect_merge_msg(MessageIterator* pmsgs, vector<merge_msg_struct*>*& vmsgs){
        map<uint64_t, merge_msg_struct*> mmsgs;
        for(; !pmsgs->done(); pmsgs->next()){
            const msg_struct& msg = pmsgs->getValue();
            if(msg.sim < 0){// ALONE type msg, sim map is empty
                auto * mmsg = new merge_msg_struct();
                mmsg->flag = msg.flag;
                mmsg->source_id = msg.i;
                mmsgs[msg.i] = mmsg;
            }else {
                auto res = mmsgs.find(msg.i);
                if (res == mmsgs.end()) {// not find, create it
                    auto * mmsg = new merge_msg_struct();
                    mmsg->flag = msg.flag;
                    mmsg->source_id = msg.i;
                    mmsgs[msg.i] = mmsg;
                }
                // merge similar
                mmsgs[msg.i]->sim.insert(pair<uint64_t, double>(msg.j, msg.sim));
            }
        }

        for (auto &mmsg : mmsgs) {
            vmsgs->push_back(mmsg.second);
        }

    }

    void send_merge_msg(uint64_t target_id, const merge_msg_struct& mmsg){
        if(mmsg.sim.empty()){
            msg_struct msg;
            msg.i = mmsg.source_id;
            msg.flag = mmsg.flag;
            msg.sim = -1;
            sendMessageTo(target_id, msg);
        }else {
            msg_struct msg;
            msg.i = mmsg.source_id;
            msg.flag = mmsg.flag;
            for (const auto &iter : mmsg.sim) {
                msg.j = iter.first;
                msg.sim = iter.second;
                sendMessageTo(target_id, msg);
            }
        }
    }

    // for test
    void output_reslut(){
        char s[100];
        auto K=VERTEX_CLASS_NAME(Graph)::k, L = VERTEX_CLASS_NAME(Graph)::l;
        if(getSuperstep() / L % 10 != 0) return;

        ofstream out;
        int n = sprintf(s, "%s/%lld.txt", VERTEX_CLASS_NAME(Graph)::logpath.c_str(), getSuperstep() / L);
        out.open(s, ios_base::app | ios_base::binary);

//        unsigned ss = sizeof(unsigned long long) * 2 + sizeof(double);
//        char* bytes = new char[ss];

        auto& value = getValue();
        if(value.sim != NULL) {
            auto vid = (unsigned long long)getVertexId();
            for(auto iter = value.sim->begin(); iter != value.sim->end(); iter++){
//                tobytes(vid, iter->first, iter->second / (getSuperstep()/ L + 1), bytes);
//                out.write(bytes, ss);
                n = sprintf(s, "%lld %lld %lf\n", vid, (unsigned long long)iter->first, iter->second / (getSuperstep() / L + 1));
                out << s;
            }
        }
    }

    char* tobytes(unsigned long long s, unsigned long long t, double v, char* bytes){
        memcpy(bytes, (char*)&s, sizeof(unsigned long long));
        memcpy(bytes + sizeof(unsigned long long), (char*)&t, sizeof(unsigned long long));
        memcpy(bytes + sizeof(unsigned long long) * 2, (char*)&v, sizeof(double));
        return bytes;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
