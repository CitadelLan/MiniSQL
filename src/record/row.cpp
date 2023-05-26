#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  /* Row结构：
   * rid_          : sizeof(RowId)
   * fields_大小    : uint32_t
   * fields_是否为空 : fields_.size() 个 *bit*
   * fields_       : Fields::SerializeTo() */

  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(),
         "Fields size do not match schema's column size.");

  uint32_t ofs = 0;
  uint32_t vctSize = fields_.size();
  std::vector<bool> null_indicator;

  /* rid_ */
  MACH_WRITE_TO(RowId, buf + ofs, rid_);
  ofs += sizeof(RowId);

  /* fields_大小 */
  MACH_WRITE_UINT32(buf + ofs, vctSize);
  ofs += sizeof(uint32_t);

  /* fields_是否为空：位图方法 0 : notNull 1 : isNull
   * !注释版本有空间浪费 */
  // for(uint32_t i = 0; i < vctSize; i++) {
  //   if (fields_[i]->IsNull()) MACH_WRITE_TO(bool, buf + ofs, true);
  //   else MACH_WRITE_TO(bool, buf + ofs, false);
  //   ofs += sizeof(bool);
  // }
  uint8_t nullBMap[vctSize/8+1];
  memset(nullBMap, 0, sizeof(nullBMap));
  for(uint32_t i = 0; i < vctSize; i++)
  {
    if(fields_[i]->IsNull())
      nullBMap[i/8] |= 1 << (i % 8);
  }
  memcpy(buf + ofs, nullBMap, sizeof(nullBMap));
  ofs += sizeof(nullBMap);

  /* fields_——非空写 */
  for(uint32_t i = 0; i < vctSize; i++)
    if(!fields_[i]->IsNull())
      ofs += fields_[i]->SerializeTo(buf + ofs);

  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  /* Row结构：
   * rid_          : sizeof(RowId)
   * fields_大小    : uint32_t
   * fields_是否为空 : fields_.size() 个 *bit*
   * fields_       : Fields::SerializeTo() */

  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");

  uint32_t ofs = 0;
  uint32_t vctSize;
  std::vector<bool> null_indicator;

  /* rid_ */
  rid_ = MACH_READ_FROM(RowId, buf + ofs);
  ofs += sizeof(RowId);

  /* fields_大小 */
  vctSize = MACH_READ_UINT32(buf + ofs);
  ofs += sizeof(uint32_t);

  /* ! fields_是否为空（位图）—— 0：notNull 1：isNull */
  null_indicator.resize(vctSize);
  uint8_t nullBMap[vctSize/8+1];
  memcpy(nullBMap, buf + ofs, sizeof(nullBMap));
  // for (uint32_t i = 0; i < vctSize; i++) {
  //   null_indicator[i] = MACH_READ_FROM(bool, buf + ofs);
  //   ofs += sizeof(bool);
  // }
  for(uint32_t i = 0; i < vctSize; i++)
    null_indicator[i] = nullBMap[i/8] & (1 << (i % 8));
  ofs += sizeof(nullBMap);

  /* fields_ —— 非空读 */
  fields_.resize(vctSize);
  for (uint32_t i = 0; i < vctSize; i++) {
    /* 对空值属性的空间分配（只对vector分配，不对row物理空间分配）*/
    if (null_indicator[i]) fields_[i] = new Field((schema->GetColumn(i))->GetType());
    /* 对非空值属性的空间分配（对vector和row物理空间分配）*/
    else {
      TypeId type = schema->GetColumn(i)->GetType();
      ofs += Field::DeserializeFrom(buf + ofs, type, &fields_[i],
                                    null_indicator[i]);
    }
  }

  null_indicator.clear();
  // null_indicator.shrink_to_fit();

  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(),
         "Fields size do not match schema's column size.");

  uint32_t ofs = 0;
  uint32_t vctSize = fields_.size();

  /* rid_ */
  ofs += sizeof(RowId);

  /* fields_数量 */
  ofs += sizeof(uint32_t);

  /* null判别位图 */
  // ofs += vctSize * sizeof(bool);
  ofs += vctSize / 8 + 1;

  /* fields */
  for(uint32_t i = 0; i < vctSize; i++)
    if(!fields_[i]->IsNull())
      ofs += fields_[i]->GetSerializedSize();

  return ofs;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
