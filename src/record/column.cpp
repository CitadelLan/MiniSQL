#include <memory>

#include "record/column.h"
#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  /* 代码运行顺序遵循如下Column结构：
   * COLUMN_MAGIC_NUM    : uint32_t
   * name长度             : uint32_t
   * name_               : name.size()
   * type_               : sizeof(type_)/GetSerializedSize
   * len_                : uint32_t
   * table_ind_          : uint32_t
   * nullable            : sizeof(bool) / 1 byte
   * unique_             : sizeof(bool) / 1 byte */

  uint32_t ofs = 0; // 写的地址offset
  uint32_t strSize = name_.size();
  // 提前保存：不保存会有内存浪费（原因未知）

  /* COLUMN_MAGIC_NUM */
  memcpy(buf, &COLUMN_MAGIC_NUM, sizeof(uint32_t));
  // MAGIC_NUM 只读，无法使用宏定义
  ofs += sizeof(uint32_t);
  /* name长度 */
  MACH_WRITE_UINT32(buf + ofs, strSize);
  ofs += sizeof(uint32_t);
  /* name_ */
  MACH_WRITE_STRING(buf + ofs, name_);
  ofs += strSize;
  /* type_ */
  MACH_WRITE_TO(TypeId, buf + ofs, type_);
  ofs += sizeof(type_);
  /* len_ */
  MACH_WRITE_UINT32(buf + ofs, len_);
  ofs += sizeof(uint32_t);
  /* table_ind_ */
  MACH_WRITE_UINT32(buf + ofs, table_ind_);
  ofs += sizeof(uint32_t);
  /* nullable */
  MACH_WRITE_TO(bool, buf + ofs, nullable_);
  ofs += sizeof(bool);
  /* unique_ */
  MACH_WRITE_TO(bool, buf + ofs, unique_);
  ofs += sizeof(bool);

  return ofs;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  return (4 * sizeof(uint32_t) + name_.size() + sizeof(type_) +
          2 * sizeof(bool));
  // (MAGIC_NUM + name长度 + table_ind_ + len_) +
  // name + type_ + (nullable +unique_)
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  /* 代码运行顺序遵循如下Column结构：
   * COLUMN_MAGIC_NUM    : uint32_t
   * name长度             : uint32_t
   * name_               : name.size()
   * type_               : sizeof(type_)/GetSerializedSize
   * len_                : uint32_t
   * table_ind_          : uint32_t
   * nullable            : sizeof(bool) / 1 byte
   * unique_             : sizeof(bool) / 1 byte */

  uint32_t ofs = 0; // 读的地址offset

  std::string name;
  TypeId type;
  uint32_t len, tb_ind, check;
  bool nullable, unique;

  /* COLUMN_MAGIC_NUM */
  check = MACH_READ_UINT32(buf);
  ASSERT(check == COLUMN_MAGIC_NUM, "Error: Deserialized non-column.");
  ofs += sizeof(uint32_t);
  /* name长度 */
  uint32_t strSize = MACH_READ_UINT32(buf + ofs);
  ofs += sizeof(uint32_t);
  /* name_ —— 作为varchar类型 */
  for(uint32_t i = 0; i < strSize; i++)
    name += MACH_READ_FROM(char, buf + ofs + i);
  ofs += strSize;
  /* type_ */
  type = MACH_READ_FROM(TypeId, buf + ofs);
  ofs += sizeof(type_);
  /* len_ */
  len = MACH_READ_UINT32(buf + ofs);
  ofs += sizeof(uint32_t);
  /* table_ind_ */
  tb_ind = MACH_READ_UINT32(buf + ofs);
  ofs += sizeof(uint32_t);
  /* nullable */
  nullable = MACH_READ_FROM(bool, buf + ofs);
  ofs += sizeof(bool);
  /* unique_ */
  unique = MACH_READ_FROM(bool, buf + ofs);
  ofs += sizeof(bool);

  /* 根据解析结果为column赋值 */
  switch(type)
  {
    /* char类型 */
    case kTypeChar:
      column = new Column(name, type, len,
                          tb_ind, nullable, unique);
      break;
    /* 数字类型 */
    case kTypeInt: case kTypeFloat:
      column = new Column(name, type, tb_ind,
                          nullable, unique);
      break;
    /* 非法类型 */
    default:
      column = nullptr; // 不能用other：other本身为nullptr
      break;
  }

  return ofs;
}
