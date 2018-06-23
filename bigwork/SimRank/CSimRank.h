//
// Created by hui on 18-6-19.
//

#ifndef CPP_GRAPHLITE_CSIMRANK_H
#define CPP_GRAPHLITE_CSIMRANK_H

#define InGraph vector<vector<uint64_t >* >
#define SimMatrix map<uint64_t, map<uint64_t, double>*> // S[i][i+1:N]
#define EPS 1e-6

#include <map>
#include "GraphLite.h"
using namespace std;
InGraph* readG(const string path){
    ifstream in;
    in.open(path.c_str(), ios_base::in);
    uint64_t num_v, num_e;
    in >> num_v;
    in >> num_e;
    InGraph* in_G = new InGraph();
    for(uint64_t i = 0; i < num_v; i++){
        in_G->push_back(new vector<uint64_t>());
    }

    uint64_t from, to;
    for(uint64_t i = 0; i < num_e; i++) {
        in >> from >> to;
        in_G->at(to)->push_back(from);
    }

    return in_G;
}

double cal_pair_sim(const InGraph* in_G, uint64_t a, uint64_t b, const SimMatrix* S, double C){
    auto Ia = in_G->at(a);
    auto Ib = in_G->at(b);
    if(Ia->empty() || Ib->empty()) return 0;
    double K = C / Ia->size() / Ib->size();
    double s = 0;
    for(auto i: *Ia){
        for(auto j: *Ib){
            if(i == j) {
                s += K;
            }else{
                if(i > j){
                    uint64_t temp = i;
                    i = j;
                    j = temp;
                }
                auto imap = S->find(i);
                if(imap != S->end()){
                    auto ijsim = imap->second->find(j);
                    if(ijsim != imap->second->end()){
                        s += K * ijsim->second;
                    }
                }
            }
        }
    }
    return s;
}

void calSim(const InGraph* in_G, double C, SimMatrix* S, const SimMatrix* oldS=NULL){
    double s = 0;
    for(uint64_t a = 0; a + 1 < in_G->size(); a++){
        for(uint64_t b = a + 1; b < in_G->size(); b++){
            if(oldS != NULL) {
                s = cal_pair_sim(in_G, a, b, oldS, C);
            }else{
                s = cal_pair_sim(in_G, a, b, S, C);
            }
            if(s > EPS){ // S is 单调递增的
                if(S->find(a) == S->end()){
                    (*S)[a] = new map<uint64_t , double>();
                }
                (*(*S)[a])[b] = s;
            }
        }
    }
}


// S is increased, so don't need clear oldS
double copyS(const SimMatrix* S, SimMatrix* oldS){
    double diff = 0;
    for(auto& row: *S){
        for(auto& it: *row.second){
            auto orow = oldS->find(row.first);
            if(orow == oldS->end()){
                (*oldS)[row.first] = new map<uint64_t, double>();
            }

            auto orow_second = (*oldS)[row.first];
            if(orow_second->find(it.first) == orow_second->end()){
                diff += abs(it.second);
            }else{
                diff += abs(it.second - (*orow_second)[it.first]);
            }
            (*orow_second)[it.first] = it.second;
        }
    }
    return diff;
}

void printG(const InGraph* in_G){
    for(uint64_t i = 0; i < in_G->size(); i++){
        cout << i << ": ";
        for(uint64_t j =0; j < in_G->at(i)->size(); j++){
            cout << in_G->at(i)->at(j) << ", ";
        }
        cout << endl;
    }
}

void printS(SimMatrix* S, string path=""){
    if(path.length() > 0) {
        ofstream cout;
        cout.open(path, ios_base::app);
        for (auto &row: *S) {
            for (auto &it: *row.second) {
                cout << row.first << " " << it.first << " " << it.second << endl;
            }
        }
    }
    else{
        for (auto &row: *S) {
            for (auto &it: *row.second) {
                cout << row.first << " " << it.first << " " << it.second << endl;
            }
        }
    }
}

#endif //CPP_GRAPHLITE_CSIMRANK_H
