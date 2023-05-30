#include <cstring>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"

char *chars[] = {const_cast<char *>(""), const_cast<char *>("hello"), const_cast<char *>("world!"),
                 const_cast<char *>("\0")};

Field int_fields[] = {
    Field(TypeId::kTypeInt, 188), Field(TypeId::kTypeInt, -65537), Field(TypeId::kTypeInt, 33389),
    Field(TypeId::kTypeInt, 0),   Field(TypeId::kTypeInt, 999),
};
Field float_fields[] = {
    Field(TypeId::kTypeFloat, -2.33f),
    Field(TypeId::kTypeFloat, 19.99f),
    Field(TypeId::kTypeFloat, 999999.9995f),
    Field(TypeId::kTypeFloat, -77.7f),
};
Field char_fields[] = {Field(TypeId::kTypeChar, chars[0], strlen(chars[0]), false),
                       Field(TypeId::kTypeChar, chars[1], strlen(chars[1]), false),
                       Field(TypeId::kTypeChar, chars[2], strlen(chars[2]), false),
                       Field(TypeId::kTypeChar, chars[3], 1, false)};
Field null_fields[] = {Field(TypeId::kTypeInt), Field(TypeId::kTypeFloat), Field(TypeId::kTypeChar)};

TEST(TupleTest, FieldSerializeDeserializeTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  char *p = buffer;
  for (int i = 0; i < 4; i++) {
    p += int_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 3; i++) {
    p += float_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 4; i++) {
    p += char_fields[i].SerializeTo(p);
  }
  // Deserialize phase
  uint32_t ofs = 0;
  Field *df = nullptr;
  for (int i = 0; i < 4; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeInt, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(int_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(int_fields[4]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(int_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(int_fields[2]));
    delete df;
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeFloat, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(float_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(float_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(float_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(float_fields[2]));
    delete df;
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeChar, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(char_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(char_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[2]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(char_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(char_fields[2]));
    delete df;
    df = nullptr;
  }
}

TEST(TupleTest, RowTest) {
  TablePage table_page;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                               Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                               Field(TypeId::kTypeFloat, 19.99f)};
  auto schema = std::make_shared<Schema>(columns);
  Row row(fields);
  table_page.Init(0, INVALID_PAGE_ID, nullptr, nullptr);
  table_page.InsertTuple(row, schema.get(), nullptr, nullptr, nullptr);
  RowId first_tuple_rid;
  ASSERT_TRUE(table_page.GetFirstTupleRid(&first_tuple_rid));
  ASSERT_EQ(row.GetRowId(), first_tuple_rid);
  Row row2(row.GetRowId());
  ASSERT_TRUE(table_page.GetTuple(&row2, schema.get(), nullptr, nullptr));
  std::vector<Field *> &row2_fields = row2.GetFields();
  ASSERT_EQ(3, row2_fields.size());
  for (size_t i = 0; i < row2_fields.size(); i++) {
    ASSERT_EQ(CmpBool::kTrue, row2_fields[i]->CompareEquals(fields[i]));
  }
  ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
  table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
}

TEST(TupleTest, RecordTest) {
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                               Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                               Field(TypeId::kTypeFloat, 19.99f)};

  char *a = new char[0xffff];
  // buf足够大，防止覆盖

  /* 1. column test */
  for (int i = 0; i < 3; i++) {
    Column *tmp;
    /* 1.1. 测试序列化长度是否正确 */
    uint32_t b = columns[i]->SerializeTo(a);
    uint32_t c = columns[i]->GetSerializedSize();
    ASSERT_EQ(b, c);
    /* 1.2. 测试反序列化长度是否正确 */
    uint32_t d = Column::DeserializeFrom(a, reinterpret_cast<Column *&>(tmp));
    ASSERT_EQ(d, c);
    /* 1.3. 测试反序列化是否正确——以GetName()为例 */
    string columnN = columns[i]->GetName(),
           tmpN = tmp->GetName();
    ASSERT_EQ(columnN, tmpN);
    delete tmp; // 释放空间
  }

  /* 2. schema test */
  Schema *schema = new Schema(columns);
  Schema *tmp;
  /* 2.1. 测试序列化长度是否正确 */
  uint32_t e = schema->SerializeTo(a);
  uint32_t f = schema->GetSerializedSize();
  ASSERT_EQ(e, f);
  /* 2.2. 测试反序列化长度是否正确 */
  uint32_t g = Schema::DeserializeFrom(a, tmp);
  ASSERT_EQ(g, f);
  /* 2.3. 测试反序列化是否正确——以GetName()为例 */
  ASSERT_EQ(schema->GetColumn(0)->GetName(), tmp->GetColumn(0)->GetName());

  /* 3. row test */
  Row *row = new Row(fields), *out = new Row();
  /* 3.1. 测试序列化长度是否正确 */
  uint32_t h = row->SerializeTo(a, schema);
  uint32_t i = row->GetSerializedSize(schema);
  ASSERT_EQ(h, i);
  /* 3.2. 测试反序列化长度是否正确 */
  uint32_t j = out->DeserializeFrom(a, tmp);
  ASSERT_EQ(j, i);
  /* 3.3. 测试反序列化是否正确——以GetName()为例 */
  ASSERT(row->GetField(0)->CompareEquals(*(out->GetField(0))), "Not equal");


  delete[] a; // 释放空间
  delete tmp;
  delete schema;
  delete row;
  delete out;
}