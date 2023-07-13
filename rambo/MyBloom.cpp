#include "MurmurHash3.h"
#include <iostream>
#include <cstring>
#include <chrono>
#include <vector>
#include "MyBloom.h"
#include <math.h>
#include "constants.h"
#include <bitset>
#include "bitArray.h"

using namespace std;

vector<uint> myhash(const std::string& key, int len, int k, int r, int range){
  // int hashvals[k];
  vector <uint> hashvals;
  hashvals.reserve(k);
  uint op; // takes 4 byte

  for (int i=0+ k*r; i<k+ k*r; i++){
    MurmurHash3_x86_32(key.c_str(), len, i, &op);
    hashvals.push_back(op%range);
  }
  return hashvals;
}

void BloomFiler::binsert(std::string key)
{
  vector<uint> temp = myhash(key,key.size(),7,0,range);
  insert(temp);
}

bool BloomFiler::btest(std::string key)
{
  vector<uint> check = myhash(key,key.size(),7,0,range);
  return test(check);
}

std::string BloomFiler::toString(){
  std::string buffer;
  buffer.reserve(range/8+1);
  buffer.append(std::string(m_bits->A,range/8+1));
  return std::move(buffer);
}

void BloomFiler::decodeFrom(const char* str, size_t str_size){
  int size_bloom=range/8+1;
  memcpy(m_bits->A,str,size_bloom);
}

BloomFiler::BloomFiler(int sz, float FPR, int k){
      p = FPR;
      k = k; //number of hash
      m_bits = new bitArray(sz);
      range=sz;
      }

void BloomFiler::insert(const vector<uint>& a){
  int N = a.size();
  for (int n =0 ; n<N; n++){
    m_bits->SetBit(a[n]);
  }
}

bool BloomFiler::test(const vector<uint>& a){
  int N = a.size();
  for (int n =0 ; n<N; n++){
      if (!m_bits->TestBit(a[n])){
        return false;
      }
  }
  return true;
}

void BloomFiler::serializeBF(string BF_file){
  m_bits->serializeBitAr(BF_file);
}

void BloomFiler::deserializeBF(vector<string> BF_file){
  m_bits->deserializeBitAr(BF_file);
}

void BloomFiler::merge_another_bf(BloomFiler *another_bf){
    m_bits->ORop(another_bf->m_bits->A);

};