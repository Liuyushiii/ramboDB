// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "db/version_set.h"
#include <unordered_map>
#include "codec/Common.h"
namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();
  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }
    meta->filter_=std::make_shared<RAMBO>(210000,3,7,100,meta->number);
    //meta->bfilter_=std::make_shared<BloomFiler>(1200000,0.01,7);
    //meta->ve=std::make_shared<std::vector<std::string>>();
    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    int64_t hh=0;
    std::string prekey="";
    //int64_t d=meta->number-5;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value());
      std::string ikey=ExtractUserKey(key).ToString().substr(0,8);
      if(ikey.compare(prekey)==0)
      {
        continue;
      }else{
        prekey=ikey;
      }
      hh++;
      //DTODO:RAMBO插入逻辑
      meta->filter_->insertion_pair(
        std::make_pair<std::string,std::string>(ExtractUserKey(key).ToString().substr(0,8),std::to_string(meta->number)));
      //baseline
      //meta->bfilter_->binsert(ikey);

      //mp
      // int64_t b=(int64_t)1 << ((d/2)%50);
      // std::string x="";
      // x.append(8,'0');
      // memcpy(&x[0],reinterpret_cast<void*>(&b) ,sizeof(int64_t));
      // std::string sum="";
      // sum.append(ikey);
      // sum.append(x);
      // meta->ve->emplace_back(sum);

    }
    //std::cout<<"size: "<<meta->ve->at(0).length()<<std::endl;
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }
    //std::cout<<"fileid: "<<meta->number<<" keynum: "<<hh<<" vesize: "<<meta->ve->size()<<std::endl;
    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
