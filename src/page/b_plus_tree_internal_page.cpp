#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_ + INTERNAL_PAGE_HEADER_SIZE)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id
 * and set max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetSize(0);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}
void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int L = 1, R = GetSize() - 1, M;

  while(L <= R)
  {
    M = (L+R)/2;  // 中间项
    int direction = KM.CompareKeys(key, KeyAt(M));  // 判断中间项与key的大小关系

    /* 按照不同比较情况决定新的二分范围
     * ！对于B+树来说：比键值小的值所在节点在左侧，大于等于其的节点在右侧！*/
    if(direction == -1)     // key小于中间项
      R = M - 1;
    else if(direction == 1) // key大于中间项
      L = M + 1;
    else                    // key等于中间项（右侧指针）
      return ValueAt(M);
  }

  return ValueAt(R);  // L一定在最后会大于R（L = R + 1），所以返回R（考虑到可能会是Value(0)）
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root page,
 * you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetSize(2);
  SetValueAt(0, old_value);
  SetKeyAt(1, new_key);
  SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value == old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int insertPos = ValueIndex(old_value) + 1;  // 插入位置

  /* 从后向前插入键与索引（防止数据流失） */
  for(int i = GetSize(); i > insertPos; i--)
  {
    SetKeyAt(i, KeyAt(i-1));
    SetValueAt(i, ValueAt((i-1)));
  }

  /* 在插入位置插入新pair */
  SetKeyAt(insertPos, new_key);
  SetValueAt(insertPos, new_value);

  IncreaseSize(1);  // 多一个元素，整体size+1

  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove latter half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  ASSERT(GetSize() > GetMaxSize(), "Error on split: Page not full");  // 因为SPLIT是在溢出时使用，这里做预判

  int end = GetSize() - 1;  // 拷贝序列末尾 = 序列末尾：下标0开始需要 -1
  int start = end / 2 + 1;  // 拷贝序列开头 = 中间项+1

  recipient->CopyNFrom(PairPtrAt(start), end - start + 1, buffer_pool_manager);

  SetSize(start); // 下标0开始，所以size = start（start自带+1）
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  int oldPgCount = GetSize();

  /* 1. 当前页信息拷贝（concat方式） */
  ASSERT((size + GetSize()) <= GetMaxSize(), "Error on copyN: page not compatible");
  PairCopy(PairPtrAt(GetSize()) , src, size);
  // 第零项无效，所以插入也无伤大雅（以n=3的B+树为例，从1插入到7，观察结果）
  IncreaseSize(size);

  /* 2. 更新子节点的父节点信息（buffer manager取页） */
  for (int i = oldPgCount; i < GetSize(); i++) {
    page_id_t pgID = ValueAt(i);
    Page* page = buffer_pool_manager->FetchPage(pgID);
    // 调用被挪位的页信息
    BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // !!!B+树每个节点都是Page类型（空间大小一致），直接转换即可
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(pgID, true);
    // 该页被访问/使用，符合unpin条件，同时由于数据遭到修改，成为了脏页
  }

  /* 所有相关的UnpinPage函数都仅在parentPageId有变动的情况下才执行，
   * 在FetchPage之后，一定会对应一个UnpinPage，在编写其他代码时请多注意 */
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  ASSERT(index >= 0, "Error on removal: negative index");
  ASSERT(GetSize() > 0, "Error on removal: Size not compatible");

  /* 前移index后的键和索引 */
  for(int i = index; i < GetSize(); i++)
  {
    SetValueAt(i, ValueAt(i+1));
    SetKeyAt(i, KeyAt(i + 1));
  }

  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  page_id_t childPgID = ValueAt(0);
  Remove(0);

  return childPgID;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 * 预设为向左合并
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  /* 1. 修改当前节点情况以适配合并，在此之后合并 */
  SetKeyAt(0, middle_key);  // *middle_key默认为正确的key*

  /* 不使用middle_key的方法 */
  // Page *parentPg = buffer_pool_manager->FetchPage(GetParentPageId());
  // BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(parentPg->GetData());  // 父节点
  // SetKeyAt(0, parent->KeyAt(ValueIndex(GetPageId())));
  /* 不使用middle_key方法的解释：
   * 合并到左侧节点后，因为recipient至少半满，所以0号下标需要填充键值。
   * 这一键值为父节点中 与指向该节点的索引为同一对 的键值
   * 因为这一键值是当前节点的最小键值（可以画一张B+树来进行检验）*/
  // buffer_pool_manager->UnpinPage(parent->GetPageId(), false);
  // parent页应该是需要进行修改的，但在这一层级的操作中，并不知道应该赋哪个值，先放这里了

  recipient->CopyNFrom(PairPtrAt(0), GetSize(), buffer_pool_manager);
  // 这里buffer已经对recipient的新增子节点的内存空间做了修改

  /* 2. 当前页报废 */
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  /* 1. 页转移 */
  recipient->CopyLastFrom(middle_key, ValueAt(0),buffer_pool_manager);
  // 这里buffer已经对recipient的新增子节点的内存空间做了修改

  /* 2. 删除头部pair：当前页size-1 */
  Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  /* 1. 插入pair */
  InsertNodeAfter(ValueAt(GetSize()-1), key, value);

  /* 2. 修改子节点信息，在buffer中更新 */
  Page *childPg = buffer_pool_manager->FetchPage(value);
  auto *child = reinterpret_cast<BPlusTreeInternalPage *>(childPg->GetData());  // 父节点
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  /* 1. 修改recipient节点情况以适配合并，在此之后合并 */
  int end = GetSize() - 1;
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(ValueAt(end), buffer_pool_manager);
  // buffer在该函数中维护了挪过去的子节点

  /* 2. 删除尾部pair：当前页size-1 */
  Remove(end);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  /* 修改子节点信息，在buffer中更新 */
  Page *childPg = buffer_pool_manager->FetchPage(value);
  auto *child = reinterpret_cast<BPlusTreeInternalPage *>(childPg->GetData());  // 父节点
  child->SetParentPageId(GetPageId());

  /* 从后向前复制——留出0号空位 */
  for(int i = GetSize(); i > 0; i--)
  {
    SetKeyAt(i, KeyAt(i-1));
    SetValueAt(i, ValueAt(i-1));
  }
  /* 填充0号空位 */
  SetValueAt(0, value);
  IncreaseSize(1);

  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}