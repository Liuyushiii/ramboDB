/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CODEC_COMMON_H_
#define CODEC_COMMON_H_

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <codec/Value.h>
namespace leveldb {

enum class PropertyType {
  UNKNOWN = 0,
  BOOL = 1,
  INT64 = 2,
  VID = 3,
  FLOAT = 4,
  DOUBLE = 5,
  STRING = 6,
  FIXED_STRING = 7,
  INT8 = 8,
  INT16 = 9,
  INT32 = 10,
};
static int64_t ramboreverse_int64(int64_t from)
{
  int64_t des=0;
  char* p_d=reinterpret_cast<char*>(&des);
  char* p_f=reinterpret_cast<char*>(&from);
  for(int i=0;i<8;i++)
  {
    p_d[7-i]=p_f[i];
  }
  return des;
}


}  // namespace leveldb
#endif  // CODEC_COMMON_H_
