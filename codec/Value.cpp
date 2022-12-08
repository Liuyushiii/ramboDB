/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "codec/Value.h"

#include <memory>
#include <string>
#include <utility>


namespace leveldb {

const Value Value::kEmpty;
const Value Value::kNullValue(NullType::__NULL__);
const Value Value::kNullNaN(NullType::NaN);
const Value Value::kNullBadData(NullType::BAD_DATA);
const Value Value::kNullBadType(NullType::BAD_TYPE);
const Value Value::kNullOverflow(NullType::ERR_OVERFLOW);
const Value Value::kNullUnknownProp(NullType::UNKNOWN_PROP);
const Value Value::kNullDivByZero(NullType::DIV_BY_ZERO);
const Value Value::kNullOutOfRange(NullType::OUT_OF_RANGE);

Value::Value(Value&& rhs) noexcept : type_(Value::Type::__EMPTY__) {
  if (this == &rhs) {
    return;
  }
  if (rhs.type_ == Type::__EMPTY__) {
    return;
  }
  switch (rhs.type_) {
    case Type::NULLVALUE: {
      setN(std::move(rhs.value_.nVal));
      break;
    }
    case Type::BOOL: {
      setB(std::move(rhs.value_.bVal));
      break;
    }
    case Type::INT: {
      setI(std::move(rhs.value_.iVal));
      break;
    }
    case Type::FLOAT: {
      setF(std::move(rhs.value_.fVal));
      break;
    }
    case Type::STRING: {
      setS(std::move(rhs.value_.sVal));
      break;
    }
  }
  rhs.clear();
}

Value::Value(const Value& rhs) : type_(Value::Type::__EMPTY__) {
  if (this == &rhs) {
    return;
  }
  if (rhs.type_ == Type::__EMPTY__) {
    return;
  }
  switch (rhs.type_) {
    case Type::NULLVALUE: {
      setN(rhs.value_.nVal);
      break;
    }
    case Type::BOOL: {
      setB(rhs.value_.bVal);
      break;
    }
    case Type::INT: {
      setI(rhs.value_.iVal);
      break;
    }
    case Type::FLOAT: {
      setF(rhs.value_.fVal);
      break;
    }
    case Type::STRING: {
      setS(*rhs.value_.sVal);
      break;
    }
  }
}

Value::Value(const NullType& v) {
  setN(v);
}

Value::Value(NullType&& v) {
  setN(std::move(v));
}

Value::Value(const bool& v) {
  setB(v);
}

Value::Value(bool&& v) {
  setB(std::move(v));
}

Value::Value(const int8_t& v) {
  setI(v);
}

Value::Value(int8_t&& v) {
  setI(std::move(v));
}

Value::Value(const int16_t& v) {
  setI(v);
}

Value::Value(int16_t&& v) {
  setI(std::move(v));
}

Value::Value(const int32_t& v) {
  setI(v);
}

Value::Value(int32_t&& v) {
  setI(std::move(v));
}

Value::Value(const int64_t& v) {
  setI(v);
}

Value::Value(int64_t&& v) {
  setI(std::move(v));
}

Value::Value(const double& v) {
  setF(v);
}

Value::Value(double&& v) {
  setF(std::move(v));
}

Value::Value(const std::string& v) {
  setS(v);
}

Value::Value(std::string&& v) {
  setS(std::move(v));
}

Value::Value(const char* v) {
  setS(v);
}

const std::string& Value::typeName() const {
  static const std::unordered_map<Type, std::string> typeNames = {
      {Type::__EMPTY__, "__EMPTY__"},
      {Type::NULLVALUE, "__NULL__"},
      {Type::BOOL, "bool"},
      {Type::INT, "int"},
      {Type::FLOAT, "float"},
      {Type::STRING, "string"},
  };

  static const std::unordered_map<NullType, std::string> nullTypes = {
      {NullType::__NULL__, "__NULL__"},
      {NullType::NaN, "NaN"},
      {NullType::BAD_DATA, "BAD_DATA"},
      {NullType::BAD_TYPE, "BAD_TYPE"},
      {NullType::ERR_OVERFLOW, "ERR_OVERFLOW"},
      {NullType::UNKNOWN_PROP, "UNKNOWN_PROP"},
      {NullType::DIV_BY_ZERO, "DIV_BY_ZERO"},
  };

  static const std::string unknownType = "__UNKNOWN__";

  auto find = typeNames.find(type_);
  if (find == typeNames.end()) {
    return unknownType;
  }
  if (find->first == Type::NULLVALUE) {
    auto nullFind = nullTypes.find(value_.nVal);
    if (nullFind == nullTypes.end()) {
      return unknownType;
    }
    return nullFind->second;
  }
  return find->second;
}

void Value::setNull(const NullType& v) {
  clear();
  setN(v);
}

void Value::setNull(NullType&& v) {
  clear();
  setN(std::move(v));
}

void Value::setBool(const bool& v) {
  clear();
  setB(v);
}

void Value::setBool(bool&& v) {
  clear();
  setB(std::move(v));
}

void Value::setInt(const int8_t& v) {
  clear();
  setI(v);
}

void Value::setInt(int8_t&& v) {
  clear();
  setI(std::move(v));
}

void Value::setInt(const int16_t& v) {
  clear();
  setI(v);
}

void Value::setInt(int16_t&& v) {
  clear();
  setI(std::move(v));
}

void Value::setInt(const int32_t& v) {
  clear();
  setI(v);
}

void Value::setInt(int32_t&& v) {
  clear();
  setI(std::move(v));
}

void Value::setInt(const int64_t& v) {
  clear();
  setI(v);
}

void Value::setInt(int64_t&& v) {
  clear();
  setI(std::move(v));
}

void Value::setFloat(const double& v) {
  clear();
  setF(v);
}

void Value::setFloat(double&& v) {
  clear();
  setF(std::move(v));
}

void Value::setStr(const std::string& v) {
  clear();
  setS(v);
}

void Value::setStr(std::string&& v) {
  clear();
  setS(std::move(v));
}

void Value::setStr(const char* v) {
  clear();
  setS(v);
}


const NullType& Value::getNull() const {
  return value_.nVal;
}

const bool& Value::getBool() const {
  return value_.bVal;
}

const int64_t& Value::getInt() const {
  return value_.iVal;
}

const double& Value::getFloat() const {
  return value_.fVal;
}

const std::string& Value::getStr() const {
  return *value_.sVal;
}



void Value::clear() {
  switch (type_) {
    case Type::__EMPTY__: {
      return;
    }
    case Type::NULLVALUE: {
      destruct(value_.nVal);
      break;
    }
    case Type::BOOL: {
      destruct(value_.bVal);
      break;
    }
    case Type::INT: {
      destruct(value_.iVal);
      break;
    }
    case Type::FLOAT: {
      destruct(value_.fVal);
      break;
    }
    case Type::STRING: {
      destruct(value_.sVal);
      break;
    }
  }
  type_ = Type::__EMPTY__;
}

Value& Value::operator=(Value&& rhs) noexcept {
  if (this == &rhs) {
    return *this;
  }
  clear();
  if (rhs.type_ == Type::__EMPTY__) {
    return *this;
  }
  switch (rhs.type_) {
    case Type::NULLVALUE: {
      setN(std::move(rhs.value_.nVal));
      break;
    }
    case Type::BOOL: {
      setB(std::move(rhs.value_.bVal));
      break;
    }
    case Type::INT: {
      setI(std::move(rhs.value_.iVal));
      break;
    }
    case Type::FLOAT: {
      setF(std::move(rhs.value_.fVal));
      break;
    }
    case Type::STRING: {
      setS(std::move(rhs.value_.sVal));
      break;
    }
  }
  rhs.clear();
  return *this;
}

Value& Value::operator=(const Value& rhs) {
  if (this == &rhs) {
    return *this;
  }
  clear();
  if (rhs.type_ == Type::__EMPTY__) {
    return *this;
  }
  switch (rhs.type_) {
    case Type::NULLVALUE: {
      setN(rhs.value_.nVal);
      break;
    }
    case Type::BOOL: {
      setB(rhs.value_.bVal);
      break;
    }
    case Type::INT: {
      setI(rhs.value_.iVal);
      break;
    }
    case Type::FLOAT: {
      setF(rhs.value_.fVal);
      break;
    }
    case Type::STRING: {
      setS(*rhs.value_.sVal);
      break;
    }
  }
  type_ = rhs.type_;
  return *this;
}

void Value::setN(const NullType& v) {
  type_ = Type::NULLVALUE;
  new (std::addressof(value_.nVal)) NullType(v);
}

void Value::setN(NullType&& v) {
  type_ = Type::NULLVALUE;
  new (std::addressof(value_.nVal)) NullType(std::move(v));
}

void Value::setB(const bool& v) {
  type_ = Type::BOOL;
  new (std::addressof(value_.bVal)) bool(v);  // NOLINT
}

void Value::setB(bool&& v) {
  type_ = Type::BOOL;
  new (std::addressof(value_.bVal)) bool(std::move(v));  // NOLINT
}

void Value::setI(const int64_t& v) {
  type_ = Type::INT;
  new (std::addressof(value_.iVal)) int64_t(v);  // NOLINT
}

void Value::setI(int64_t&& v) {
  type_ = Type::INT;
  new (std::addressof(value_.iVal)) int64_t(std::move(v));  // NOLINT
}

void Value::setF(const double& v) {
  type_ = Type::FLOAT;
  new (std::addressof(value_.fVal)) double(v);  // NOLINT
}

void Value::setF(double&& v) {
  type_ = Type::FLOAT;
  new (std::addressof(value_.fVal)) double(std::move(v));  // NOLINT
}

void Value::setS(std::unique_ptr<std::string> v) {
  type_ = Type::STRING;
  new (std::addressof(value_.sVal)) std::unique_ptr<std::string>(std::move(v));
}

void Value::setS(const std::string& v) {
  type_ = Type::STRING;
  new (std::addressof(value_.sVal)) std::unique_ptr<std::string>(new std::string(v));
}

void Value::setS(std::string&& v) {
  type_ = Type::STRING;
  new (std::addressof(value_.sVal)) std::unique_ptr<std::string>(new std::string(std::move(v)));
}

void Value::setS(const char* v) {
  type_ = Type::STRING;
  new (std::addressof(value_.sVal)) std::unique_ptr<std::string>(new std::string(v));
}

}  // namespace leveldb
