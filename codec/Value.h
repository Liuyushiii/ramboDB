/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CODEC_VALUE_H_
#define CODEC_VALUE_H_

#include <memory>
#include <unordered_map>

namespace leveldb {

enum class NullType {
  __NULL__ = 0,
  NaN = 1,
  BAD_DATA = 2,
  BAD_TYPE = 3,
  ERR_OVERFLOW = 4,
  UNKNOWN_PROP = 5,
  DIV_BY_ZERO = 6,
  OUT_OF_RANGE = 7,
};

struct Value {
  static const Value kEmpty;
  static const Value kNullValue;
  static const Value kNullNaN;
  static const Value kNullBadData;
  static const Value kNullBadType;
  static const Value kNullOverflow;
  static const Value kNullUnknownProp;
  static const Value kNullDivByZero;
  static const Value kNullOutOfRange;

  static const uint64_t kEmptyNullType;
  static const uint64_t kNumericType;


  enum class Type : uint64_t {
    __EMPTY__ = 1UL,
    BOOL = 1UL << 1,
    INT = 1UL << 2,
    FLOAT = 1UL << 3,
    STRING = 1UL << 4,
    NULLVALUE = 1UL << 63,
  };

  // Constructors
  Value() : type_(Type::__EMPTY__) {}
  Value(Value&& rhs) noexcept;
  Value(const Value& rhs);

  // For the cpp bool-pointer conversion, if Value ctor accept a pointer without
  // matched ctor it will convert to bool type and the match the bool value
  // ctor, So we disable all pointer ctor except the char*
  template <typename T>
  Value(T*) = delete;                    // NOLINT
  Value(const std::nullptr_t) = delete;  // NOLINT
  Value(const NullType& v);              // NOLINT
  Value(NullType&& v);                   // NOLINT
  Value(const bool& v);                  // NOLINT
  Value(bool&& v);                       // NOLINT
  Value(const int8_t& v);                // NOLINT
  Value(int8_t&& v);                     // NOLINT
  Value(const int16_t& v);               // NOLINT
  Value(int16_t&& v);                    // NOLINT
  Value(const int32_t& v);               // NOLINT
  Value(int32_t&& v);                    // NOLINT
  Value(const int64_t& v);               // NOLINT
  Value(int64_t&& v);                    // NOLINT
  Value(const double& v);                // NOLINT
  Value(double&& v);                     // NOLINT
  Value(const std::string& v);           // NOLINT
  Value(std::string&& v);                // NOLINT
  Value(const char* v);                  // NOLINT
  ~Value() {
    clear();
  }

  Type type() const noexcept {
    return type_;
  }

  const std::string& typeName() const;
  static const std::string toString(Type type);

  bool empty() const {
    return type_ == Type::__EMPTY__;
  }
  bool isNull() const {
    return type_ == Type::NULLVALUE;
  }
  bool isBadNull() const {
    if (!isNull()) {
      return false;
    }
    auto& null = value_.nVal;
    return null == NullType::NaN || null == NullType::BAD_DATA || null == NullType::BAD_TYPE ||
           null == NullType::ERR_OVERFLOW || null == NullType::UNKNOWN_PROP ||
           null == NullType::DIV_BY_ZERO || null == NullType::OUT_OF_RANGE;
  }
  bool isNumeric() const {
    return type_ == Type::INT || type_ == Type::FLOAT;
  }
  bool isBool() const {
    return type_ == Type::BOOL;
  }
  bool isInt() const {
    return type_ == Type::INT;
  }
  bool isFloat() const {
    return type_ == Type::FLOAT;
  }
  bool isStr() const {
    return type_ == Type::STRING;
  }

  void clear();

  void __clear() {
    clear();
  }

  Value& operator=(Value&& rhs) noexcept;
  Value& operator=(const Value& rhs);

  void setNull(const NullType& v);
  void setNull(NullType&& v);
  void setBool(const bool& v);
  void setBool(bool&& v);
  void setInt(const int8_t& v);
  void setInt(int8_t&& v);
  void setInt(const int16_t& v);
  void setInt(int16_t&& v);
  void setInt(const int32_t& v);
  void setInt(int32_t&& v);
  void setInt(const int64_t& v);
  void setInt(int64_t&& v);
  void setFloat(const double& v);
  void setFloat(double&& v);
  void setStr(const std::string& v);
  void setStr(std::string&& v);
  void setStr(const char* v);

  const NullType& getNull() const;
  const bool& getBool() const;
  const int64_t& getInt() const;
  const double& getFloat() const;
  const std::string& getStr() const;
  
 private:
  Type type_;

  union Storage {
    NullType nVal;
    bool bVal;
    int64_t iVal;
    double fVal;
    std::unique_ptr<std::string> sVal;
    Storage() {}
    ~Storage() {}
  } value_;

  template <class T>
  void destruct(T& val) {
    (&val)->~T();
  }

  // Null value
  void setN(const NullType& v);
  void setN(NullType&& v);
  // Bool value
  void setB(const bool& v);
  void setB(bool&& v);
  // Integer value
  void setI(const int64_t& v);
  void setI(int64_t&& v);
  // Double float value
  void setF(const double& v);
  void setF(double&& v);
  // String value
  void setS(const std::string& v);
  void setS(std::string&& v);
  void setS(const char* v);
  void setS(std::unique_ptr<std::string> v);
};

}  // namespace leveldb

#endif  // CODEC_VALUE_H_
