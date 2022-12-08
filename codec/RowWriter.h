/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CODEC_ROWWRITER_H_
#define CODEC_ROWWRITER_H_

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <cmath>
#include "codec/Common.h"
#include "codec/ResultSchemaProvider.h"
#include "leveldb/db.h"

namespace leveldb {

enum class WriteResult {
  SUCCEEDED = 0,
  UNKNOWN_FIELD = -1,
  TYPE_MISMATCH = -2,
  OUT_OF_RANGE = -3,
  NOT_NULLABLE = -4,
  FIELD_UNSET = -5,
  INCORRECT_VALUE = -6,
};

/********************************************************************************

  Encoder version 2

  This is the second version of encoder. It is not compatible with the first
  version. The purpose of this new version is to improve the read performance

  The first byte of the encoded string indicates which version of the encoder
  used to encode the properties. Here is the explanation of the header byte

  Version 1:
                 v v v 0 0 b b b
    In version 1, the middle two bits are always zeros. The left three bits
    indicates the number of bytes used for the schema version, while the right
    three bits indicates the number of bytes used for the block offsets

  Version 2:
                 0 0 0 0 1 v v v
    In version 2, the left three bits are reserved. The middle two bits
    indicate the encoder version, and the right three bits indicate the number
    of bytes used for the schema version

  The biggest change from version 1 to version 2 is every property now has a
  fixed length. As long as you know the property type, you know how many bytes
  the property occupies. In other words, given the sequence number of the
  property, you get the start point of the property in O(1) time.

  The types supported by the encoder version 2 are:
        BOOL            (1 byte)
        INT8            (1 byte)
        INT16           (2 bytes)
        INT32           (4 bytes)
        INT64           (8 bytes)
        FLOAT           (4 bytes)
        DOUBLE          (8 bytes)
        STRING          (8 bytes) *
        FIXED_STRING    (Length defined in the schema)
        TIMESTAMP       (8 bytes)
        DATE            (4 bytes)
        DATETIME        (15 bytes)
        GEOGRAPHY       (8 bytes) *

  All except STRING typed properties are stored in-place. The STRING property
  stored the offset of the string content in the first 4 bytes and the length
  of the string in the last 4 bytes. The string content is appended to the end
  of the encoded string

  The encoder version 2 also supports the NULL value for all types. It uses
  one bit flag to indicate whether a property is NULL if the property is
  nullable. So one byte can represent NULL values for 8 nullable properties.
  The total number of bytes needed for the NULL flags is

  ((number_of_nullable_props - 1) >> 3) + 1

  Here is the overall byte sequence for the encoding

       int64            不用            fixed_string  int64                     先不用可变string
    <edgeNumber> edgeNumbers{pos_offset}  {<dstId>  <version> <all properties> <string content>}s
         |               |                    |         |             |             |           |
       8 bytes    edgenumber*4bytes         8 bytes  8 bytes      N bytes

********************************************************************************/
class RowWriter {
 public:
  explicit RowWriter(ResultSchemaProvider* schema, int64_t edgeNumber);
  //explicit RowWriter(RowReader& reader);

  RowWriter(ResultSchemaProvider* schema, std::string&& encoded);
  ~RowWriter() = default;

  /**
   * @brief Return the exact length of the encoded binary array
   *
   * @return int64_t
   */
  int64_t size() const noexcept {
    return buf_.size();
  }

  /**
   * @brief Return the related schema
   *
   * @return const meta::SchemaProviderIf*
   */
  const ResultSchemaProvider* schema() const {
    return schema_;
  }

  /**
   * @brief Get the encoded string
   *
   * @return const std::string&
   */
  const std::string& getEncodedStr() const noexcept {
    //CHECK(finished_) << "You need to call finish() first";
    return buf_;
  }

  /**
   * @brief Get the encoded string with move
   *
   * @return std::string
   */
  std::string moveEncodedStr() noexcept {
    //CHECK(finished_) << "You need to call finish() first";
    return std::move(buf_);
  }

  int64_t getEdgenumber() noexcept{
    return edgeNumber_;
  }

  // Data write
  /**
   * @brief Set propertyfield value by index
   *
   * @tparam T
   * @param index Field index
   * @param v Value to write
   * @return WriteResult
   */
  template <typename T>
  WriteResult set(size_t index, T&& v) noexcept {
    //CHECK(!finished_) << "You have called finish()";
    if (index >= schema_->getNumFields()) {
      return WriteResult::UNKNOWN_FIELD;
    }
    return write(index, std::forward<T>(v));
  }

  // Data write
  /**
   * @brief Set property value by property name
   *
   * @tparam T
   * @param name Property name
   * @param v Value to write
   * @return WriteResult
   */
  template <typename T>
  WriteResult set(const std::string& name, T&& v) noexcept {
    //CHECK(!finished_) << "You have called finish()";
    int64_t index = schema_->getFieldIndex(name);
    if (index >= 0) {
      return write(static_cast<size_t>(index), std::forward<T>(v));
    } else {
      return WriteResult::UNKNOWN_FIELD;
    }
  }

  /**
   * @brief Set the number of edges 
   *
   * @param dstId
   * @return WriteResult
   */
  WriteResult writeNumber(int64_t v) noexcept;
  /**
   * @brief Set dstID for each edge
   *
   * @param dstId
   * @return WriteResult
   */
  WriteResult setDstId(int64_t pos, const std::string& dstId) noexcept;
  /**
   * @brief Set the value by index
   *
   * @param index
   * @param val
   * @return WriteResult
   */
  WriteResult setVersion(int64_t pos, const int64_t version) noexcept;
  /**
   * @brief Set the value by index
   *
   * @param index
   * @param val
   * @return WriteResult
   */
  WriteResult setMultiValue(ssize_t pos, ssize_t index, const Value& val) noexcept;

  /**
   * @brief Set the value by index
   *
   * @param index
   * @param val
   * @return WriteResult
   */
  WriteResult setMultiValue(ssize_t pos, const std::string& name, const Value& val) noexcept;

  WriteResult appendWriter(std::string&& buf) noexcept;

  WriteResult delWriter(int64_t posl, int64_t posr) noexcept;

  Value getPosValueByName(int64_t pos, const std::string& prop) const noexcept ;
  Value getPosValueByIndex(int64_t pos, const int64_t index) const noexcept ;
  Value getPosDstId(int64_t pos) const noexcept ;
  Value getPosVersion(int64_t pos) const noexcept ;
  
 private:
  ResultSchemaProvider* schema_;
  std::string buf_;
  // The number of bytes occupied by header and the schema version
  size_t headerLen_;
  size_t numNullBytes_;
  size_t approxStrLen_;
  size_t sumStrLen_=0; //所有可变string的长度
  size_t vidLen_;
  int64_t edgeNumber_;
  bool finished_;

  // When outOfSpaceStr_ is true, variant length string fields
  // could hold an index, referring to the strings in the strList_
  // By default, outOfSpaceStr_ is false. It turns true only when
  // the existing variant length string is modified
  bool outOfSpaceStr_;
  std::vector<std::string> strList_;


  //多边写入
  WriteResult writeDstId(int64_t pos, Slice v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, bool v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, float v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, double v) noexcept;

  WriteResult multiwrite(int64_t pos, ssize_t index, int8_t v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, int16_t v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, int32_t v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, int64_t v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, uint8_t v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, uint16_t v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, uint32_t v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, uint64_t v) noexcept;
  

  WriteResult multiwrite(int64_t pos, ssize_t index, const std::string& v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, Slice v) noexcept;
  WriteResult multiwrite(int64_t pos, ssize_t index, const char* v) noexcept;
};

}  // namespace leveldb
#endif  // CODEC_RowWriter_H_
