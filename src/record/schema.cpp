#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  /* Schema结构：
   * SCHEMA_MAGIC_NUM : uint32_t
   * columns数目       : uint32_t （）
   * columns          : 调用columns[i]->SerializeTo()
   * is_manage_       : sizeof(bool) */

  uint32_t ofs = 0;

  /* COLUMN_MAGIC_NUM */
  memcpy(buf, &SCHEMA_MAGIC_NUM, sizeof(uint32_t));
  // MAGIC_NUM 只读，无法使用宏定义
  ofs += sizeof(uint32_t);

  /* columns数目 */
  MACH_WRITE_UINT32(buf + ofs, columns_.size());
  ofs += sizeof(uint32_t);

  /* columns */
  for(auto it : columns_)
    ofs += it->SerializeTo(buf+ofs);

  /* unique_ */
  MACH_WRITE_TO(bool, buf + ofs, is_manage_);
  ofs += sizeof(bool);

  return ofs;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t ofs = 0;

  /* COLUMN_MAGIC_NUM + columns个数 */
  ofs += 2 * sizeof(uint32_t);

  /* columns */
  for(auto it : columns_)
    ofs += it->GetSerializedSize();

  /* unique_ */
  ofs += sizeof(bool);

  return ofs;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  /* Schema结构：
   * SCHEMA_MAGIC_NUM : uint32_t
   * columns          : 调用columns[i]->SerializeTo()
   * is_manage_       : sizeof(bool) */

  uint32_t ofs = 0;

  uint32_t check;
  uint32_t columnsNum;
  std::vector<Column *> columns;
  bool isManage;

  /* COLUMN_MAGIC_NUM */
  check = MACH_READ_UINT32(buf);
  ASSERT(check == SCHEMA_MAGIC_NUM, "Error: Deserialized non-schema.");
  ofs += sizeof(uint32_t);

  /* columns 数目 */
  columnsNum = MACH_READ_UINT32(buf+ofs);
  ofs += sizeof (uint32_t);

  /* columns */
  columns.resize(columnsNum);
  for(uint32_t i = 0; i < columnsNum; i++)
    ofs += Column::DeserializeFrom(buf + ofs, columns[i]);

  /* is_manage_ */
  isManage = MACH_READ_FROM(bool, buf + ofs);
  ofs += sizeof(bool);

  schema = new Schema(columns, isManage);

  columns.clear();
  // columns.shrink_to_fit();  // pending

  return ofs;
}