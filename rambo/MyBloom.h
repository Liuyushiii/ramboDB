#ifndef _MYBLOOM_
#define _MYBLOOM_
#include <vector>
#include "constants.h"
#include <bitset>
#include "bitArray.h"

std::vector<uint> myhash(const std::string& key, int len, int k, int r, int range);

class BloomFiler{
    public:
        // BloomFiler(int capacity, float FPR, int k);
        BloomFiler(int sz, float FPR, int k);
        void insert(const std::vector<uint>& a);
        bool test(const std::vector<uint>& a);
        void serializeBF(std::string BF_file);
        void deserializeBF(std::vector<std::string> BF_file);
        void merge_another_bf(BloomFiler *another_bf);
        void binsert(std::string key);
        bool btest(std::string key);
        std::string toString();
        void decodeFrom(const char* str, size_t str_size);
        // void serialize1(std::string BF_file);

        int n;
        float p;
        int R;
        int k;
        int range;
        // std::vector<bool> m_bits;
        // std::bitset<capacity> m_bits;
        bitArray* m_bits;
};

#endif
