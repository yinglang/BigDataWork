/* 0, 2017E8009361008, YuXuehui */
#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) SSSP##name

#define INF numeric_limits<double>::max()
#define EPS 1e-6

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter{
public:
    int getVertexValueSize() override {
        m_n_value_size = sizeof(double);
        return m_n_value_size;
    }

    int getEdgeValueSize() override {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }

    int getMessageValueSize() override {
        m_m_value_size = sizeof(double);
        return m_m_value_size;
    }

    int64_t getVertexNum() override {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex = n;
        return m_total_vertex;
    }

    int64_t getEdgeNum() override {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge = n;
        return m_total_edge;
    }

    void loadGraph() override {
        unsigned long long from, to, last_vertex;
        double weight;
        int outdegree = 0;
        double value = INF;

        const char* line = getEdgeLine();
        sscanf(line, "%lld %lld %lf", &from, &to, &weight);
        addEdge(from, to, &weight);
        last_vertex = from;
        outdegree ++;

        for(int i = 0; i < m_total_edge; i++){
            line = getEdgeLine();
            sscanf(line, "%lld %lld %lf", &from, &to, &weight);
            if(from != last_vertex){
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            }else {
                outdegree++;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &value, outdegree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter{
public:
    void writeResult() override {
        int64_t vid;
        double value;
        char s[1024];
        int n;

        for(ResultIterator r_iter; !r_iter.done(); r_iter.next()){
            r_iter.getIdValue(vid, &value);
            if(value < INF) {
                n = sprintf(s, "%lld: %f\n", (unsigned long long) vid, value);
            }else{
                n = sprintf(s, "%lld: INF\n", (unsigned long long) vid);
            }
            writeNextResLine(s, n);
        }
    }
};

template <typename AggrValue>
class TAggregator: public AggregatorBase{
public:
    AggrValue m_global; /**< aggregator global value of AggrValue type */
    AggrValue m_local;  /**< aggregator local value of AggrValue type */

public:
    int getSize() const override {
        return sizeof(AggrValue);
    }

    void init() override {
        m_global = 0;
        m_local = 0;
    }

    void *getGlobal() override {
        return &m_global;
    }

    void setGlobal(const void *p) override {
        m_global = *(AggrValue *)p;
    }

    void *getLocal() override {
        return &m_local;
    }

    void merge(const void* p) override {}

    void accumulate(const void* p) override {}
};

class VERTEX_CLASS_NAME(Aggregator): public TAggregator<bool>{
public:
    void init() override {
        m_global = true;
        m_local = true;
    }

    void merge(const void *notUpdate) override {
        m_global &= *(bool*)notUpdate;
    }

    void accumulate(const void * notUpdate) override {
        m_local &= *(bool*)notUpdate;
    }
};

class VERTEX_CLASS_NAME(): public Vertex<double, double, double>{
public:
    void compute(MessageIterator *pmsgs) override {
        /**
         * super step i > 0:
         *  1. collect short path length from neighbour (INF neighbour will be ignore.)
         *  2. find min short path in collected path.
         *  3. if short path < INF, send shot path + edge length to neighbor; else ignore.
         * */
        printf("%lld compute\n", (unsigned long long)getVertexId());
        double min_length = INF;
        if(getSuperstep() == 0){
            // init, source vertex send short path to neighbor
            int64_t source = *(int64_t *)getAggrGlobal(0);
            if(getVertexId() == source){
                for(auto iter = getOutEdgeIterator(); !iter.done(); iter.next()){
                    double v = iter.getValue();
                    if(v < INF) sendMessageTo(iter.target(), v);
                }
            }
        }else{
            if(getSuperstep() >= 2){       // stop condition
                if(getSuperstep() == getVSize() && *(bool*)getAggrGlobal(1)){
                    voteToHalt();
                    return;
                }
            }
            for(;!pmsgs->done(); pmsgs->next()){
                if(pmsgs->getValue() < min_length){
                    min_length = pmsgs->getValue();
                }
            }
            bool notUpdate = (min_length == getValue());
            accumulateAggr(1, &notUpdate);
        }

        if(min_length < INF) {
            *(mutableValue()) = min_length;
            for (auto iter = getOutEdgeIterator(); !iter.done(); iter.next()) {
                if(iter.getValue() < INF) {
                    double v = iter.getValue() + min_length;
                    sendMessageTo(iter.target(), v);
                }
            }
        }
    }
};

class VERTEX_CLASS_NAME(Graph) : public Graph{
public:
    VERTEX_CLASS_NAME(Aggregator)* notUpdate;
    TAggregator<int64_t>* source;
public:
    void init(int argc, char* argv[]) override {
        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if(argc < 4){
            printf("Usage: %s <input path> <output path> <v0 id>", argv[0]);
            exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
        unsigned long long n;
        sscanf(argv[3], "%lld", &n);

        // registry aggregator
        regNumAggr(2);
        source = new TAggregator<int64_t>[1];
        int64_t source_id = n;
        source->setGlobal(&source_id);
        regAggr(0, &source[0]);

        notUpdate = new VERTEX_CLASS_NAME(Aggregator)[1];
        regAggr(1, &notUpdate[0]);
    }

    void term() override {
        delete[] source;
        delete[] notUpdate;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph(){
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph)();

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pgraph){
    delete (VERTEX_CLASS_NAME()*) pgraph->m_pver_base;
    delete (VERTEX_CLASS_NAME(InputFormatter)*) pgraph->m_pin_formatter;
    delete (VERTEX_CLASS_NAME(OutputFormatter)*) pgraph->m_pout_formatter;
    delete (VERTEX_CLASS_NAME(Graph)*) pgraph;
}
