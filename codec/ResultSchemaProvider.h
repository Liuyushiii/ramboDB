/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CODEC_RESULTSCHEMAPROVIDER_H_
#define CODEC_RESULTSCHEMAPROVIDER_H_

#include "codec/Common.h"
#include <vector>
#include <string.h>

namespace leveldb {

class ResultSchemaProvider{
 public:
  class ResultSchemaField{
   public:
    ResultSchemaField(std::string name,
                      PropertyType type,
                      int16_t size,
                      int32_t offset,
                      std::string defaultValue = "");

    const char* name() const ;
    PropertyType type() const ;
    bool hasDefault() const ;
    const std::string& defaultValue() const ;
    size_t size() const ;
    size_t offset() const ;

   private:
    std::string name_;
    PropertyType type_;
    int16_t size_;
    int32_t offset_;
    std::string defaultValue_;
  };

 public:
  ResultSchemaProvider();
  virtual ~ResultSchemaProvider() = default;

  /*SchemaVer getVersion() const noexcept  {
    return schemaVer_;
  }*/
  void insertResultSchemaProvider(ResultSchemaField rsf);
  size_t getNumFields() const noexcept ;

  size_t size() const noexcept ;

  int64_t getFieldIndex(const std::string& name) const ;
  const char* getFieldName(int64_t index) const ;

  PropertyType getFieldType(int64_t index) const ;
  PropertyType getFieldType(const std::string& name) const ;

  const ResultSchemaField* field(int64_t index) const ;
  const ResultSchemaField* field(const std::string& name) const ;

 protected:
  //SchemaVer schemaVer_{0};

  std::vector<ResultSchemaField> columns_;
  // Map of Hash64(field_name) -> array index
  std::unordered_map<std::string, int64_t> nameIndex_;
  size_t numNullableFields_{0};

  // Default constructor, only used by SchemaWriter
  //explicit ResultSchemaProvider(SchemaVer ver = 0) : schemaVer_(ver) {}
};

}  // namespace leveldb
#endif  // CODEC_RESULTSCHEMAPROVIDER_H_
