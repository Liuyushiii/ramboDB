/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "codec/ResultSchemaProvider.h"

namespace leveldb {


/***********************************
 *
 * ResultSchemaField
 *
 **********************************/
ResultSchemaProvider::ResultSchemaField::ResultSchemaField(std::string name,
                                                           PropertyType type,
                                                           int16_t size,
                                                           int32_t offset,
                                                           std::string defaultValue)
    : name_(std::move(name)),
      type_(type),
      size_(size),
      offset_(offset),
      defaultValue_(defaultValue) {}

const char* ResultSchemaProvider::ResultSchemaField::name() const {
  return name_.c_str();
}

PropertyType ResultSchemaProvider::ResultSchemaField::type() const {
  return type_;
}

bool ResultSchemaProvider::ResultSchemaField::hasDefault() const {
  return defaultValue_ != "";
}

const std::string& ResultSchemaProvider::ResultSchemaField::defaultValue() const {
  return defaultValue_;
}

size_t ResultSchemaProvider::ResultSchemaField::size() const {
  return size_;
}

size_t ResultSchemaProvider::ResultSchemaField::offset() const {
  return offset_;
}


/***********************************
 *
 * ResultSchemaProvider
 *
 **********************************/
ResultSchemaProvider::ResultSchemaProvider(){
  ResultSchemaProvider::ResultSchemaField from_account("from_account", PropertyType::FIXED_STRING, 8, 0, "");
  ResultSchemaProvider::ResultSchemaField to_account("to_account", PropertyType::FIXED_STRING, 8, 8, "");
  ResultSchemaProvider::ResultSchemaField value("value", PropertyType::INT64, 8, 16, "");
  insertResultSchemaProvider(from_account);
  insertResultSchemaProvider(to_account);
  insertResultSchemaProvider(value);
  // ResultSchemaProvider edge;
  // edge.insertResultSchemaProvider(from_account);
  // edge.insertResultSchemaProvider(to_account);
  // edge.insertResultSchemaProvider(value);
  // edge.insertResultSchemaProvider(version);
  // return edge;
}

void ResultSchemaProvider::insertResultSchemaProvider(ResultSchemaField rsf){
  columns_.push_back(rsf);
  if(columns_.size()!=0){
    nameIndex_.emplace(std::make_pair(rsf.name(), columns_.size() - 1));
  }
  else{
    nameIndex_.emplace(std::make_pair(rsf.name(), 0));
  }
}
size_t ResultSchemaProvider::getNumFields() const noexcept {
  return columns_.size();
}

size_t ResultSchemaProvider::size() const noexcept {
  if (columns_.size() > 0) {
    auto& last = columns_.back();
    return last.offset() + last.size();
  } else {
    return 0;
  }
}

int64_t ResultSchemaProvider::getFieldIndex(const std::string& name) const {
  auto iter = nameIndex_.find(name);
  if (iter == nameIndex_.end()) {
    return -1;
  }
  return iter->second;
}

const char* ResultSchemaProvider::getFieldName(int64_t index) const {
  if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
    return nullptr;
  }
  return columns_[index].name();
}

PropertyType ResultSchemaProvider::getFieldType(int64_t index) const {
  if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
    return PropertyType::UNKNOWN;
  }

  return columns_[index].type();
}

PropertyType ResultSchemaProvider::getFieldType(const std::string& name) const {
  auto index = getFieldIndex(name);
  if (index < 0) {
    return PropertyType::UNKNOWN;
  }
  return columns_[index].type();
}

const ResultSchemaProvider::ResultSchemaField* ResultSchemaProvider::field(int64_t index) const {
  if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
    return nullptr;
  }
  return &(columns_[index]);
}

const ResultSchemaProvider::ResultSchemaField* ResultSchemaProvider::field(const std::string& name) const {
  auto index = getFieldIndex(name);
  if (index < 0) {
    return nullptr;
  }
  return &(columns_[index]);
}

}  // namespace nebula
