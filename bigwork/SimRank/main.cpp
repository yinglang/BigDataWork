//
// Created by hui on 18-6-12.
//

/**
 * running pipeline:
 * 1. . setenv
 * 2. start-graphlite cmake-build-debug/libPageRankVertex.so Input/facebookcombined_4w Output/out
 */

#include <map>
#include <iostream>
#include <vector>
#include <fstream>

using namespace std;

typedef struct {
    double a;               // 8B
    string s;               // 32B, not include pointer's memory
}MsgType1;

typedef struct {
    double a;               // 8B
    string s="hallodddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";       // 32B
}MsgType2;

typedef struct {
    double a;               // 8B
    string s="hallo";       // 32B
    map<int, double> score; // 48B
}MsgType3;

typedef struct {
    double a;               // 8B
    const char *s= "hallo00000000000000";       // 8B, 64bit OS 's pointer is 8B, dynamic memory, it is const value, cannot be change, and all const are same memory.
}MsgType4;

typedef struct {
    double a;               // 8B
    char s[4];              // 8B 边界对齐, 8x
}MsgType5;

typedef struct {
    double a;               // 8B
    const char *s= new char[3];       // 8B, 64bit OS 's pointer is 8B, dynamic memory
}MsgType6;

vector<vector<int>> get(){
    vector<vector<int>> res;
    for(int i = 0; i < 5; i++) {
        vector<int> v;
        for(int j = 0; j < i+1; j++) {
            v.push_back(i);
        }
        res.push_back(v);
    }
    return res;
}

void test1(){
    std::map<unsigned long long, double> score;
    score[1] = 0.1;
    score[2] = 0.2;

    // map assign is deep copy
    auto score2 = score;
    score2[1] = 1;

    cout << score[1] << " " << score[2] << endl;

    cout << "Msg1 size:"  << sizeof(MsgType1) << endl;
    cout << "Msg2 size:"  << sizeof(MsgType2) << endl;
    cout << "Msg3 size:"  << sizeof(MsgType3) << endl;
    cout << "Msg4 size:"  << sizeof(MsgType4) << endl;
    cout << "Msg5 size:"  << sizeof(MsgType5) << endl;
    cout << "Msg6 size:"  << sizeof(MsgType6) << endl;

    MsgType4 m41;
    MsgType4 m42;
    cout << (void*)(m41.s) << " " << (void*)(m42.s) << endl;

    MsgType6 m;
    MsgType6 m2;
    cout << (void*)(m.s) << " " << (void*)(m2.s) << endl;
    m2 = m;
    cout << (void*)(m.s) << " " << (void*)(m2.s) << endl;

    auto res = get();
    for(auto& v: res){
        for(auto& i: v){
            cout << i << " ";
        }
        cout << endl;
    }

    vector<int> v1;
    vector<int> v2 = v1;
    v1.push_back(1);
    cout << v2.size() << endl;
}

#include "CSimRank.h"

char* tobytes(unsigned long long s, unsigned long long t, double v, char* bytes){
    memcpy(bytes, (char*)&s, sizeof(unsigned long long));
    memcpy(bytes + sizeof(unsigned long long), (char*)&t, sizeof(unsigned long long));
    memcpy(bytes + sizeof(unsigned long long) * 2, (char*)&v, sizeof(double));
    return bytes;
}

int main(){
    ofstream out;
    out.open("/home/hui/github/BigDataWork/bigwork/SimRank/log.bin", ios_base::binary | ios_base::out);
    unsigned ss = sizeof(unsigned long long) * 2 + sizeof(double);
    char* s = new char[ss];
    tobytes(0, 1, 0.52, s);
    out.write(s, ss);
    tobytes(100, 99, 0.33, s);
    out.write(s, ss);

//    auto in_G = readG("/home/hui/github/BigDataWork/bigwork/SimRank/Input/facebookcombined"); //testgraph" facebookcombined);
//    printG(in_G);
//    cout << endl;
//
//    clock_t start = clock();
//
//    SimMatrix* S = new SimMatrix();
//    SimMatrix* oldS = new SimMatrix();
//    for(int k = 0; k < 100; k ++) {
//        calSim(in_G, 0.8, S, oldS);     // update S sync
//        //calSim(in_G, 0.8, S);           // update S async
//        double diff = copyS(S, oldS);
//        cout << k << " " << diff << endl;
//        if(diff <= EPS) break;
//    }
//    cout << endl;
//    clock_t end = clock();
//    cout << "cost time:" << (double)(end-start) / CLOCKS_PER_SEC << "s." << endl;
//
//    printS(S, "/home/hui/github/BigDataWork/bigwork/SimRank/log/naive_simrank_facebook_sync.txt");
}
