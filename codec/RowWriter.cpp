/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "codec/RowWriter.h"

#include <cmath>



namespace leveldb {


RowWriter::RowWriter(ResultSchemaProvider* schema, int64_t edgeNumber)
    : schema_(schema), numNullBytes_(0), approxStrLen_(0),sumStrLen_(0), vidLen_(8),edgeNumber_(edgeNumber) , finished_(false), outOfSpaceStr_(false) {

  // Reserve space for the header, the data, and the string values
  buf_.reserve(8 + (schema_->size()+vidLen_+8)* edgeNumber+1024);

  headerLen_=0;

  // edgeNumber

  // Reserve the space for the data, including the Null bits
  // All variant length string will be appended to the end
  buf_.resize(numNullBytes_ + 8 + (schema_->size()+vidLen_+8)* edgeNumber, '\0');
  auto x=writeNumber(edgeNumber);
}

RowWriter::RowWriter(ResultSchemaProvider* schema, std::string&& encoded)
    : schema_(schema), finished_(false), outOfSpaceStr_(false) {
  auto len = encoded.size();
  buf_ = std::move(encoded);
  headerLen_ = 0;
  numNullBytes_ = 0;
  vidLen_=8;
  // int64_t vv;
  // memcpy(reinterpret_cast<char*>(&vv),&buf_[0],sizeof(int64_t));
  // std::cout<<"edge: "<<vv<<std::endl;
  memcpy(reinterpret_cast<char*>(&edgeNumber_), &buf_[0], sizeof(int64_t));
}

WriteResult RowWriter::appendWriter(std::string&& buf) noexcept { 
  int64_t edgenumber;
  memcpy(reinterpret_cast<void*>(&edgenumber), &buf[0], sizeof(int64_t));
  //替换edgenumber
  edgeNumber_+=edgenumber;
  auto x=writeNumber(edgeNumber_);
  /*
  //替换原来的edge's offset的值
  int32_t pos_offset,offset;
  offset=8+edgeNumber_*4;
  pos_offset=8;
  for(int64_t i=1;i<=edgeNumber_-edgenumber;i++)
  {
    memcpy(&buf_[pos_offset], reinterpret_cast<void*>(&offset), sizeof(int32_t));
    pos_offset+=4;
    offset+=(schema_->size()+8*2);
  }
  //插入新的,这个insert操作是O(n)的，可能会拖慢速度
  std::string tmp;
  for(int64_t i=edgeNumber_-edgenumber+1 ;i<=edgeNumber_ ;i++)
  {
    tmp.append(reinterpret_cast<char*>(&offset),sizeof(int32_t));
    offset+=(schema_->size()+8*2);
  }
  buf_.insert(pos_offset,tmp);
  */
  //把buf上的edge's offset append到原有的后面
  buf_.append(buf.substr(8));
  return WriteResult::SUCCEEDED;
}

WriteResult RowWriter::delWriter(int64_t posl, int64_t posr) noexcept{
  int64_t edgeNumber=posr-posl+1;
  std::string buf="";
  buf.reserve(8 + (schema_->size()+vidLen_+8)* edgeNumber);
  memcpy(&buf[0], reinterpret_cast<void*>(&edgeNumber), sizeof(int64_t));
  /*
  int32_t pos_offset,offset;
  offset=8+edgeNumber_*4;
  pos_offset=8;
  for(int64_t i=1;i<=edgeNumber;i++)
  {

  }
  */
  buf.append(buf_.substr(8+(posl-1)*(schema_->size()+vidLen_+8),(schema_->size()+vidLen_+8)* edgeNumber));
  buf_=std::move(buf);
  return WriteResult::SUCCEEDED;
}

WriteResult RowWriter::writeNumber(int64_t v) noexcept {
  auto offset = headerLen_ + numNullBytes_;
  if (v > std::numeric_limits<int64_t>::max() || v < std::numeric_limits<int64_t>::min()) {
    return WriteResult::OUT_OF_RANGE;
  }
  int64_t iv = v;
  memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
  return WriteResult::SUCCEEDED;
}


WriteResult RowWriter::setDstId(int64_t pos, const std::string& dstId) noexcept{
  return writeDstId(pos,Slice(dstId));
}

WriteResult RowWriter::writeDstId(int64_t pos, Slice v) noexcept {
  auto offset = headerLen_ + numNullBytes_ + 8 +(pos-1)*(schema_->size()+vidLen_+8)+ sumStrLen_;
  strncpy(&buf_[offset], v.data(), vidLen_);
  /*
  //record the edge's content with offset
  auto pos_offset=headerLen_ + numNullBytes_ + 8 + (pos-1)*4;
  memcpy(&buf_[pos_offset], reinterpret_cast<void*>(&offset), sizeof(int32_t));
  */
  return WriteResult::SUCCEEDED;
}

Value RowWriter::getPosDstId(int64_t pos) const noexcept {
  size_t pos_offset= 8 /*+ edgeNumber_*4*/ + (pos-1)*(schema_->size()+vidLen_+8);

  return std::string(&buf_[pos_offset], sizeof(int64_t));
}


WriteResult RowWriter::setVersion(int64_t pos, const int64_t version) noexcept {
  size_t offset = headerLen_ + numNullBytes_ + 8 /*+ edgeNumber_*4*/ +(pos-1)*(schema_->size()+vidLen_+8)+ vidLen_ + sumStrLen_;
  if (version > std::numeric_limits<int64_t>::max() || version < std::numeric_limits<int64_t>::min()) {
    return WriteResult::OUT_OF_RANGE;
  }
  int64_t iv = version;
  memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
  return WriteResult::SUCCEEDED;
}

Value RowWriter::getPosVersion(int64_t pos) const noexcept {
  size_t pos_offset= 8 /*+ edgeNumber_*4*/ + (pos-1)*(schema_->size()+vidLen_+8)+vidLen_;
  int64_t val;
  memcpy(reinterpret_cast<void*>(&val), &buf_[pos_offset], sizeof(int64_t));
  return val;
}


WriteResult RowWriter::setMultiValue(ssize_t pos, ssize_t index, const Value& val) noexcept {
  //CHECK(!finished_) << "You have called finish()";
  if (index < 0 || static_cast<size_t>(index) >= schema_->getNumFields()) {
    return WriteResult::UNKNOWN_FIELD;
  }

  switch (val.type()) {
    case Value::Type::BOOL:
      return multiwrite( pos,index, val.getBool());
    case Value::Type::INT:
      return multiwrite( pos,index, val.getInt());
    case Value::Type::FLOAT:
      return multiwrite( pos,index, val.getFloat());
    case Value::Type::STRING:
      return multiwrite( pos,index, val.getStr());
    default:
      return WriteResult::TYPE_MISMATCH;
  }
}

WriteResult RowWriter::setMultiValue(ssize_t pos, const std::string& name, const Value& val) noexcept {
  //CHECK(!finished_) << "You have called finish()";
  int64_t index = schema_->getFieldIndex(name);
  return setMultiValue(pos, index, val);
}

Value RowWriter::getPosValueByName(int64_t pos, const std::string& prop) const noexcept {
  int64_t index = schema_->getFieldIndex(prop);
  return getPosValueByIndex(pos, index);
}

Value RowWriter::getPosValueByIndex(int64_t pos, const int64_t index) const noexcept {
  if (index < 0 || static_cast<size_t>(index) >= schema_->getNumFields()) {
    return Value(NullType::UNKNOWN_PROP);
  }

  auto field = schema_->field(index);
  size_t pos_offset= 8 /*+ edgeNumber_*4*/ + (pos-1)*(schema_->size()+vidLen_+8);
  
  size_t offset = pos_offset + vidLen_/*DstId*/ + 8/*Version*/+ field->offset();

  switch (field->type()) {
    case PropertyType::BOOL: {
      if (buf_[offset]) {
       // Value b(true);
        return true;
      } else {
        return false;
      }
    }
    case PropertyType::INT8: {
      return static_cast<int8_t>(buf_[offset]);
    }
    case PropertyType::INT16: {
      int16_t val;
      memcpy(reinterpret_cast<void*>(&val), &buf_[offset], sizeof(int16_t));
      return val;
    }
    case PropertyType::INT32: {
      int32_t val;
      memcpy(reinterpret_cast<void*>(&val), &buf_[offset], sizeof(int32_t));
      return val;
    }
    case PropertyType::INT64: {
      int64_t val;
      memcpy(reinterpret_cast<void*>(&val), &buf_[offset], sizeof(int64_t));
      return val;
    }
    case PropertyType::VID: {
      // This is to be compatible with V1, so we treat it as
      // 8-byte long string
      return std::string(&buf_[offset], sizeof(int64_t));
    }
    case PropertyType::FLOAT: {
      float val;
      memcpy(reinterpret_cast<void*>(&val), &buf_[offset], sizeof(float));
      return val;
    }
    case PropertyType::DOUBLE: {
      double val;
      memcpy(reinterpret_cast<void*>(&val), &buf_[offset], sizeof(double));
      return val;
    }
    case PropertyType::STRING: {
      int32_t strOffset;
      int32_t strLen;
      memcpy(reinterpret_cast<void*>(&strOffset), &buf_[offset], sizeof(int32_t));
      memcpy(reinterpret_cast<void*>(&strLen), &buf_[offset + sizeof(int32_t)], sizeof(int32_t));
      if (static_cast<size_t>(strOffset) == buf_.size() && strLen == 0) {
        return std::string();
      }
      return std::string(&buf_[strOffset], strLen);
    }
    case PropertyType::FIXED_STRING: {
      return std::string(&buf_[offset], field->size());
    }
    case PropertyType::UNKNOWN:
      break;
  }
}


WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, bool v) noexcept {
  auto field = schema_->field(index);
  auto offset = headerLen_ + numNullBytes_ + 8 /*+ edgeNumber_*4*/ + (pos-1)*(schema_->size()+vidLen_+8)+vidLen_+8+ sumStrLen_ +field->offset();
  switch (field->type()) {
    case PropertyType::BOOL:
    case PropertyType::INT8:
      buf_[offset] = v ? 0x01 : 0;
      break;
    case PropertyType::INT64:
      buf_[offset + 7] = 0;
      buf_[offset + 6] = 0;
      buf_[offset + 5] = 0;
      buf_[offset + 4] = 0;  // fallthrough
    case PropertyType::INT32:
      buf_[offset + 3] = 0;
      buf_[offset + 2] = 0;  // fallthrough
    case PropertyType::INT16:
      buf_[offset + 1] = 0;
      buf_[offset + 0] = v ? 0x01 : 0;
      break;
    default:
      return WriteResult::TYPE_MISMATCH;
  }
  return WriteResult::SUCCEEDED;
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, float v) noexcept {
  auto field = schema_->field(index);
  auto offset = headerLen_ + numNullBytes_ + 8 /*+ edgeNumber_*4*/ +(pos-1)*(schema_->size()+vidLen_+8)+vidLen_+8+ sumStrLen_ +field->offset();
  switch (field->type()) {
    case PropertyType::INT8: {
      if (v > std::numeric_limits<int8_t>::max() || v < std::numeric_limits<int8_t>::min()) {
        return WriteResult::OUT_OF_RANGE;
      }
      int8_t iv = std::round(v);
      buf_[offset] = iv;
      break;
    }
    case PropertyType::INT16: {
      if (v > std::numeric_limits<int16_t>::max() || v < std::numeric_limits<int16_t>::min()) {
        return WriteResult::OUT_OF_RANGE;
      }
      int16_t iv = std::round(v);
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int16_t));
      break;
    }
    case PropertyType::INT32: {
      if (v > static_cast<float>(std::numeric_limits<int32_t>::max()) ||
          v < static_cast<float>(std::numeric_limits<int32_t>::min())) {
        return WriteResult::OUT_OF_RANGE;
      }
      int32_t iv = std::round(v);
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int32_t));
      break;
    }
    case PropertyType::INT64: {
      if (v > static_cast<float>(std::numeric_limits<int64_t>::max()) ||
          v < static_cast<float>(std::numeric_limits<int64_t>::min())) {
        return WriteResult::OUT_OF_RANGE;
      }
      int64_t iv = std::round(v);
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
      break;
    }
    case PropertyType::FLOAT: {
      memcpy(&buf_[offset], reinterpret_cast<void*>(&v), sizeof(float));
      break;
    }
    case PropertyType::DOUBLE: {
      double dv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&dv), sizeof(double));
      break;
    }
    default:
      return WriteResult::TYPE_MISMATCH;
  }
  return WriteResult::SUCCEEDED;
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, double v) noexcept {
  auto field = schema_->field(index);
  auto offset = headerLen_ + numNullBytes_ + 8 /*+ edgeNumber_*4*/ +(pos-1)*(schema_->size()+vidLen_+8)+vidLen_+8+ sumStrLen_ + field->offset();
  switch (field->type()) {
    case PropertyType::INT8: {
      if (v > std::numeric_limits<int8_t>::max() || v < std::numeric_limits<int8_t>::min()) {
        return WriteResult::OUT_OF_RANGE;
      }
      int8_t iv = std::round(v);
      buf_[offset] = iv;
      break;
    }
    case PropertyType::INT16: {
      if (v > std::numeric_limits<int16_t>::max() || v < std::numeric_limits<int16_t>::min()) {
        return WriteResult::OUT_OF_RANGE;
      }
      int16_t iv = std::round(v);
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int16_t));
      break;
    }
    case PropertyType::INT32: {
      if (v > std::numeric_limits<int32_t>::max() || v < std::numeric_limits<int32_t>::min()) {
        return WriteResult::OUT_OF_RANGE;
      }
      int32_t iv = std::round(v);
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int32_t));
      break;
    }
    case PropertyType::INT64: {
      if (v > static_cast<double>(std::numeric_limits<int64_t>::max()) ||
          v < static_cast<double>(std::numeric_limits<int64_t>::min())) {
        return WriteResult::OUT_OF_RANGE;
      }
      int64_t iv = std::round(v);
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
      break;
    }
    case PropertyType::FLOAT: {
      if (v > std::numeric_limits<float>::max() || v < std::numeric_limits<float>::lowest()) {
        return WriteResult::OUT_OF_RANGE;
      }
      float fv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&fv), sizeof(float));
      break;
    }
    case PropertyType::DOUBLE: {
      memcpy(&buf_[offset], reinterpret_cast<void*>(&v), sizeof(double));
      break;
    }
    default:
      return WriteResult::TYPE_MISMATCH;
  }
  return WriteResult::SUCCEEDED;
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, uint8_t v) noexcept {
  return multiwrite( pos,index, static_cast<int8_t>(v));
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, int8_t v) noexcept {
  auto field = schema_->field(index);
  auto offset = headerLen_ + numNullBytes_ + 8 /*+ edgeNumber_*4*/ +(pos-1)*(schema_->size()+vidLen_+8)+vidLen_+8+ sumStrLen_+ field->offset();
  switch (field->type()) {
    case PropertyType::BOOL: {
      buf_[offset] = v == 0 ? 0x00 : 0x01;
      break;
    }
    case PropertyType::INT8: {
      buf_[offset] = v;
      break;
    }
    case PropertyType::INT16: {
      int16_t iv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int16_t));
      break;
    }
    case PropertyType::INT32: {
      int32_t iv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int32_t));
      break;
    }
    case PropertyType::INT64: {
      int64_t iv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
      break;
    }
    case PropertyType::FLOAT: {
      float fv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&fv), sizeof(float));
      break;
    }
    case PropertyType::DOUBLE: {
      double dv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&dv), sizeof(double));
      break;
    }
    default:
      return WriteResult::TYPE_MISMATCH;
  }
  return WriteResult::SUCCEEDED;
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, uint16_t v) noexcept {
  return multiwrite( pos,index, static_cast<int16_t>(v));
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, int16_t v) noexcept {
  auto field = schema_->field(index);
  auto offset = headerLen_ + numNullBytes_ + 8 /*+ edgeNumber_*4*/ +(pos-1)*(schema_->size()+vidLen_+8)+vidLen_+8+ sumStrLen_+ field->offset();
  switch (field->type()) {
    case PropertyType::BOOL: {
      buf_[offset] = v == 0 ? 0x00 : 0x01;
      break;
    }
    case PropertyType::INT8: {
      if (v > std::numeric_limits<int8_t>::max() || v < std::numeric_limits<int8_t>::min()) {
        return WriteResult::OUT_OF_RANGE;
      }
      int8_t iv = v;
      buf_[offset] = iv;
      break;
    }
    case PropertyType::INT16: {
      memcpy(&buf_[offset], reinterpret_cast<void*>(&v), sizeof(int16_t));
      break;
    }
    case PropertyType::INT32: {
      int32_t iv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int32_t));
      break;
    }
    case PropertyType::INT64: {
      int64_t iv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
      break;
    }
    case PropertyType::FLOAT: {
      float fv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&fv), sizeof(float));
      break;
    }
    case PropertyType::DOUBLE: {
      double dv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&dv), sizeof(double));
      break;
    }
    default:
      return WriteResult::TYPE_MISMATCH;
  }
  return WriteResult::SUCCEEDED;
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, uint32_t v) noexcept {
  return multiwrite(pos,index, static_cast<int32_t>(v));
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, int32_t v) noexcept {
  auto field = schema_->field(index);
  auto offset = headerLen_ + numNullBytes_ + 8 /*+ edgeNumber_*4*/ +(pos-1)*(schema_->size()+vidLen_+8)+vidLen_+8+ sumStrLen_+ field->offset();
  switch (field->type()) {
    case PropertyType::BOOL: {
      buf_[offset] = v == 0 ? 0x00 : 0x01;
      break;
    }
    case PropertyType::INT8: {
      if (v > std::numeric_limits<int8_t>::max() || v < std::numeric_limits<int8_t>::min()) {
        return WriteResult::OUT_OF_RANGE;
      }
      int8_t iv = v;
      buf_[offset] = iv;
      break;
    }
    case PropertyType::INT16: {
      if (v > std::numeric_limits<int16_t>::max() || v < std::numeric_limits<int16_t>::min()) {
        return WriteResult::OUT_OF_RANGE;
      }
      int16_t iv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int16_t));
      break;
    }
    case PropertyType::INT32: {
      memcpy(&buf_[offset], reinterpret_cast<void*>(&v), sizeof(int32_t));
      break;
    }
    case PropertyType::INT64: {
      int64_t iv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
      break;
    }
    case PropertyType::FLOAT: {
      float fv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&fv), sizeof(float));
      break;
    }
    case PropertyType::DOUBLE: {
      double dv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&dv), sizeof(double));
      break;
    }
    default:
      return WriteResult::TYPE_MISMATCH;
  }
  return WriteResult::SUCCEEDED;
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, uint64_t v) noexcept {
  return multiwrite(pos,index, static_cast<int64_t>(v));
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, int64_t v) noexcept {
  auto field = schema_->field(index);
  auto offset = headerLen_ + numNullBytes_ + 8 /*+ edgeNumber_*4*/ +(pos-1)*(schema_->size()+vidLen_+8)+vidLen_+8+ sumStrLen_+ field->offset();
  switch (field->type()) {
    case PropertyType::BOOL: {
      buf_[offset] = v == 0 ? 0x00 : 0x01;
      break;
    }
    case PropertyType::INT8: {
      if (v > std::numeric_limits<int8_t>::max() || v < std::numeric_limits<int8_t>::min()) {
        return WriteResult::OUT_OF_RANGE;
      }
      int8_t iv = v;
      buf_[offset] = iv;
      break;
    }
    case PropertyType::INT16: {
      if (v > std::numeric_limits<int16_t>::max() || v < std::numeric_limits<int16_t>::min()) {
        return WriteResult::OUT_OF_RANGE;
      }
      int16_t iv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int16_t));
      break;
    }
    case PropertyType::INT32: {
      if (v > std::numeric_limits<int32_t>::max() || v < std::numeric_limits<int32_t>::min()) {
        return WriteResult::OUT_OF_RANGE;
      }
      int32_t iv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int32_t));
      break;
    }
    case PropertyType::INT64: {
      memcpy(&buf_[offset], reinterpret_cast<void*>(&v), sizeof(int64_t));
      break;
    }
    case PropertyType::FLOAT: {
      float fv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&fv), sizeof(float));
      break;
    }
    case PropertyType::DOUBLE: {
      double dv = v;
      memcpy(&buf_[offset], reinterpret_cast<void*>(&dv), sizeof(double));
      break;
    }
    default:
      return WriteResult::TYPE_MISMATCH;
  }
  return WriteResult::SUCCEEDED;
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, const std::string& v) noexcept {
  return multiwrite(pos,index, Slice(v));
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, const char* v) noexcept {
  return multiwrite(pos,index, Slice(v));
}

WriteResult RowWriter::multiwrite(int64_t pos,ssize_t index, Slice v) noexcept {
  auto field = schema_->field(index);
  auto offset = headerLen_ + numNullBytes_ + 8 /*+ edgeNumber_*4*/ +(pos-1)*(schema_->size()+vidLen_+8)+vidLen_+8+ sumStrLen_+ field->offset();
  switch (field->type()) {
    case PropertyType::STRING: {
      int32_t strOffset;
      int32_t strLen;
      if (outOfSpaceStr_) {
        strList_.emplace_back(v.data(), v.size());
        strOffset = 0;
        // Length field is the index to the out-of-space string list
        strLen = strList_.size() - 1;
      } else {
        // Append to the end
        //这里逻辑要改一下，不能append最后，要insert到对应的位置
        strOffset=headerLen_ + numNullBytes_ +2 /*+ edgeNumber_*4*/ +pos*(schema_->size()+vidLen_)+ sumStrLen_;
        buf_.insert(strOffset,v.data());
        strLen = v.size();
        //buf_.append(v.data(), strLen);
      }
      memcpy(&buf_[offset], reinterpret_cast<void*>(&strOffset), sizeof(int32_t));
      memcpy(&buf_[offset + sizeof(int32_t)], reinterpret_cast<void*>(&strLen), sizeof(int32_t));
      sumStrLen_+=v.size();
      approxStrLen_ += v.size();
      break;
    }
    case PropertyType::FIXED_STRING: {
      // In-place string. If the pass-in string is longer than the pre-defined
      // fixed length, the string will be truncated to the fixed length
      size_t len = v.size() > field->size() ? field->size() : v.size();
      strncpy(&buf_[offset], v.data(), len);
      if (len < field->size()) {
        memset(&buf_[offset + len], 0, field->size() - len);
      }
      break;
    }
    default:
      return WriteResult::TYPE_MISMATCH;
  }
  return WriteResult::SUCCEEDED;
}

}  // namespace nebula
