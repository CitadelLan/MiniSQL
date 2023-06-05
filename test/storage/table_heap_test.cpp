#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 1000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(),
                                            nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    auto *fields =
        new Fields{Field(TypeId::kTypeInt, i),
                   Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   // 析构时free
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
    }
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values) {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    delete row_kv.second;
  }
  ASSERT_EQ(size, 0);

  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
}

TEST(TableHeapTest, MyTHTest)
{
  /* 0. TableHeap 初始化 */
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 1000;
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(),
                                            nullptr, nullptr, nullptr);
  /* 1. 检验InsertTuple UpdateTuple */
  std::unordered_map<int64_t, Fields> row_values;
  float tmp = 1;
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = "SoloWing";
    auto fields = Fields{Field(TypeId::kTypeInt, i),
                   Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   // 析构时free
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));

    row_values.emplace(row.GetRowId().Get(), fields);
    // delete fields;
  }

  /* 2. 检验Begin()(侧面检验GetTuple()) End() it++(侧面检验++it) */
  int i = 0;
  for(auto it = table_heap->Begin(nullptr); it != table_heap->End(); ++it, i++)
  {
    // EXPECT_EQ(it->GetRowId(), RID[i]); // 用RID比较不合适，因为插入顺序不等于空间存储顺序
    EXPECT_EQ(CmpBool::kTrue, it->GetField(0)->CompareEquals(row_values[it->GetRowId().Get()].at(0)));
  }
  ASSERT_EQ(i, row_nums);

  /* 3. 检验UpdateTuple() */
  /* 3.0. 创建新row */
  int32_t len = 20;
  char *characters = "kestrel";
  auto fields = Fields{Field(TypeId::kTypeInt, 1),
                 Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                 // 析构时free
                 Field(TypeId::kTypeFloat, tmp)};
  Row row(fields);
  /* 3.1. 实际更新操作与验证 */
  auto HRID = table_heap->Begin(nullptr)->GetRowId();
  ASSERT_TRUE(table_heap->UpdateTuple(row, HRID, nullptr));
  auto head = table_heap->Begin(nullptr);
  ASSERT_EQ(CmpBool::kTrue, head->GetField(0)->CompareEquals(fields.at(0)));

  /* 4. 检验ApplyDelete与Begin()的重新调整 */
  head++;
  table_heap->MarkDelete(HRID, nullptr);
  table_heap->ApplyDelete(HRID, nullptr);
  auto newHead = table_heap->Begin(nullptr);
  ASSERT_EQ(CmpBool::kTrue, newHead->GetField(0)->CompareEquals(row_values[head->GetRowId().Get()].at(0)));
  newHead = table_heap->End();  // 避免double free

  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
}
