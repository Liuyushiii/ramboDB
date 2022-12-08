/* Copyright (c) 2011 The LevelDB Authors. All rights reserved.
   Use of this source code is governed by a BSD-style license that can be
   found in the LICENSE file. See the AUTHORS file for names of contributors. */

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "db/version_edit.h"
#include "leveldb/write_batch.h"
#include "db/memtable.h"
#include "codec/Common.h"

#include <openssl/sha.h>
#include <rambo/Rambo_construction.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

#define ACCOUNT_RANGE 1000000

std::string sha256(const std::string &srcStr) {
  unsigned char mdStr[33] = {0};
  SHA256((const unsigned char *)srcStr.c_str(), srcStr.length(), mdStr);
  return std::string((char *)mdStr, 32);
}


void getNextState(leveldb::Random &rand, std::string &key, std::string &value,int64_t version) {
  std::string srcStr = std::to_string(rand.Next() % ACCOUNT_RANGE);
  // key = sha256(srcStr).substr(0, 20);
  key=srcStr.substr(0,8);
  key=std::string(8-key.length(),'0')+key;
  int64_t vv=leveldb::ramboreverse_int64(version);
  key.append(reinterpret_cast<char*>(&vv),sizeof(int64_t));
  value = key;
}

std::map<std::string,std::vector<int>> key_cache;
void generateWriteBatch(int batch_size_, int batch_num_, leveldb::WriteBatch &write_batch_) {
  static leveldb::Random rand(1);
  std::string key;
  std::string value;
  std::string pos;
  for (int i = 0; i < batch_size_; i++) {
      getNextState(rand, key, value, batch_num_);
      key_cache[key.substr(0,key.size()-8)].push_back(batch_num_);
      pos = std::to_string(batch_num_);
      write_batch_.Put(key, value, batch_num_);
  }
  return;
}

void ramboTest(){
  RAMBO rambo_filter(100000,3,10,100,0);
  // rambo_filter.createMetaRambo_single(0);
  rambo_filter.insertion_pair(std::make_pair<std::string,std::string>("abc","0"));
  rambo_filter.insertion_pair(std::make_pair<std::string,std::string>("edf","0"));
  rambo_filter.createMetaRambo_single(1);
  rambo_filter.insertion_pair(std::make_pair<std::string,std::string>("bcd","1"));

  std::cout<<"Query"<<std::endl;
  auto res_set=rambo_filter.query_bias_set("bcd",3);
  for(auto& fid:res_set){
    std::cout<<fid<<std::endl;
  }
}

void dbTest(){
  std::cout<<"1"<<std::endl;
  // leveldb::Env* env=leveldb::Env::Default();
  
  leveldb::DB* db=nullptr;
  leveldb::Options opts;
  opts.create_if_missing = true;
  leveldb::Status s=leveldb::DB::Open(opts,"/tmp/test",&db);
  std::cout<<"2:"<<s.ToString()<<std::endl;
  leveldb::WriteBatch batch1;
  

  std::cout<<"3"<<std::endl;
  std::string db_state;
  for(int i=0;i<100;++i){
    generateWriteBatch(2000,i,batch1);
    db->Write(leveldb::WriteOptions(),&batch1);
    db->GetProperty("leveldb.stats", &db_state);
    batch1.Clear();
    std::cout<<i<<db_state<<std::endl;
  }
  std::cout<<"4:"<<s.ToString()<<std::endl;

  leveldb::ReadOptions read_option;
  read_option.min_height=0;
  read_option.max_height=100;
  for(auto& kv:key_cache){
    std::string value;
    db->Get(read_option,kv.first,&value);
    std::cout<<"key:"<<kv.first<<std::endl;
    std::cout<<"value:"<<value<<std::endl;
    std::cout<<"s-value:";
    for(int bid:kv.second){
      if(bid>read_option.max_height){
        break;
      }
      std::cout<<bid<<" ";
    }
    std::cout<<std::endl;
    break;
  }

  delete db;
}


int main(int argc, char** argv) {
  // ramboTest();
  dbTest();
  return 0;
}
