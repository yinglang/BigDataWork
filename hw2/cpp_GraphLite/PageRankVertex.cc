/**
 * @file PageRankVertex.cc
 * @author  Songjie Niu, Shimin Chen
 * @version 0.1
 *
 * @section LICENSE
 *
 * Copyright 2016 Shimin Chen (chensm@ict.ac.cn) and
 * Songjie Niu (niusongjie@ict.ac.cn)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @section DESCRIPTION
 *
 * This file implements the PageRank algorithm using graphlite API.
 *
 */

/**
 * you need to define
 * 1. InputFormatter    --> loadGraph (add Vertex and Edge by read input file), get*ValueSize (set V/E/M value's type size), get*Num (read V/E numbers)
 * 2. OutputFormatter   --> writeResult (output result to output file)
 * 3. Vertex            --> compute (the core function)
 * 4. Graph             --> init (set input/output file name from command argv, registry aggregator), term (release dynamic memory)
 *
 * automatic function
 * 1. create_graph(create Graph and set InputFormatter/OutputFormatter/Vertex class)
 * 2. destroy_graph(release memory)
 * */

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) PageRankVertex##name

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
        sscanf(m_ptotal_vertex_line, "%lld", &n);  // first line is vertex num
        m_total_vertex = n;
        return m_total_vertex;
    }

    int64_t getEdgeNum() override {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);   // second line is edge num
        m_total_edge = n;
        return m_total_edge;
    }

    void loadGraph(){
        unsigned long long last_vertex;
        unsigned long long from, to;
        double weight = 0;
        double value = 1;
        int outdegree = 0;

        const char* line = getEdgeLine();
        sscanf(line, "%lld %lld", &from, &to);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++ outdegree;
        for(int64_t i = 1; i < m_total_edge; i++){
            line = getEdgeLine();
            sscanf(line, "%lld %lld", &from, &to);
            if(last_vertex != from){
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            }else{
                outdegree ++;
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

        for(ResultIterator r_iter; !r_iter.done(); r_iter.next()){
            r_iter.getIdValue(vid, &value);
            int n = sprintf(s, "%lld: %f\n", (unsigned long long)vid, value);
            writeNextResLine(s, n);
        }
    }
};

class VERTEX_CLASS_NAME(Aggregator): public Aggregator<double>{
public:
    void init() override {
        m_global = 0;
        m_local = 0;
    }

    void *getGlobal() override {
        return &m_global;
    }

    void setGlobal(const void *p) override {
        m_global = *(double *) p;
    }

    void *getLocal() override {
        return &m_local;
    }

    void merge(const void *p) override {
        m_global += *(double*) p;
    }

    void accumulate(const void *p) override {
        m_local += *(double *)p;
    }
};

#define EPS 1e-6
// V value, E value, M value
class VERTEX_CLASS_NAME(): public Vertex<double, double, double>{
public:
    void compute(MessageIterator *pmsgs) override {
        double val;
        if(getSuperstep() == 0){ // init
            val = 1.0;
        }else{
            if(getSuperstep() >= 2){ // stop condition
                double global_val = * (double*) getAggrGlobal(0);
                if(global_val < EPS){
                    voteToHalt();
                    return;
                }
            }

            double sum = 0;
            for(; ! pmsgs->done(); pmsgs->next()){  // core cal
                sum += pmsgs->getValue();
            }
            val = 0.15 + 0.85 * sum;

            double acc = fabs(getValue() - val);
            accumulateAggr(0, &acc);                        // id 0 aggregator
        }

        * mutableValue() = val;
        const int64_t n = getOutEdgeIterator().size();
        sendMessageToAllNeighbors(val / n);
    }
};

class VERTEX_CLASS_NAME(Graph):public Graph{
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: PageRankVertex.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    void init(int argc, char* argv[]){
        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if(argc < 3){
            printf("Usage: %s <input path> <output path>\n", argv[0]);
            exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
    }

    void term(){
        delete[] aggregator;
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
