#ifndef _RamboConstruction_
#define _RamboConstruction_
#include <iomanip>
#include <fstream>
#include <iostream>
#include <chrono>
#include <vector>
#include <math.h>
#include <sstream>
#include <string>
#include <string.h>
#include <algorithm>
#include "MyBloom.h"
#include "MurmurHash3.h"
#include "utils.h"
#include "constants.h"
#include "bitArray.h"
#include <set>
#include <iterator>
#include <bitset>
#include <unordered_set>
#include <boost/dynamic_bitset.hpp>
#include <thread>
#include <future>
#include <tbb/tbb.h>
// vector<uint> hashfunc( void* key, int len, int R, int B){
// }

class RAMBO {
public:

    RAMBO(int n=100000, int r1=3, int b1=10, int K=100,int bias_=0);

    std::vector<uint> hashfunc(std::string key, int len);

    std::vector<std::string> getdata(std::string filenameSet);

    void serializeRAMBO(std::string dir);

    std::string toString();

    bool decodeFrom(const char* str,size_t str_size);

    void deserializeRAMBO(std::vector<std::string> dir);

    void insertion_pairs(std::vector<std::pair<std::string, std::string>> &data_key_number);

    void createMetaRambo_single(int value, int64_t bit);

    //void createMetaRambo_single(int value);
    void insertion_pair(std::pair<std::string, std::string> pair1);

    void merge_another_rambo(RAMBO &b);

    void out_set();
    boost::dynamic_bitset<> query_bias(std::string query_key, int len, int bias);
    std::set<int> query_bias_set(std::string query_key, int len);


    int R;
    int B;
    int n;
    float p;
    int range;
    int k;
    float FPR;
    int K1;
    int bias;
    BloomFiler **Rambo_array;
    //std::vector<int> *metaRambo;
    //std::unordered_set<int> *metaRambo;
    int64_t *metaRambo;
    
    
};

#endif
