#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_ + LEAF_PAGE_HEADER_SIZE)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  max_size = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (sizeof(std::pair<GenericKey *, page_id_t>)) - 1;
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);  // 测试中发现好像没有成功置-1，显示添加一下
  SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
    int L = 0, R = GetSize() - 1, M;

    while(L <= R)
    {
        M = (L + R)/2;  // 中间项
        int direction = KM.CompareKeys(key, KeyAt(M));  // 判断中间项与key的大小关系

        /* 按照不同比较情况决定新的二分范围
         * ！对于B+树来说：比键值小的值所在节点在左侧，大于等于其的节点在右侧！*/
        if(direction == -1)     // key小于中间项
            R = M - 1;
        else if(direction == 1) // key大于中间项
            L = M + 1;
        else                    // key等于中间项（右侧指针）
            return M;
    }

    return L;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    // replace with your own code
    return make_pair(KeyAt(index), ValueAt(index));
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {

    int i;
    int target = KeyIndex(key, KM);
    /* 从后向前更新key和rid，直到目的key > key[i]， 或者插入index为0 */
    for(i = GetSize(); i > target; i--){
        SetKeyAt(i, KeyAt(i-1));
        SetValueAt(i, ValueAt(i-1));
    }

    /* 插入key和value,更新Size为size+1 */
    SetKeyAt(i, key);
    SetValueAt(i, value);
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
    int end = GetSize() - 1;
    int start = end / 2 + 1;
    recipient->CopyNFrom(PairPtrAt(start), end - start + 1);
    SetSize(start);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
    ASSERT((size + GetSize()) <= GetMaxSize(), "Error on copyN: page not compatible");
    PairCopy(PairPtrAt(GetSize()), src, size);
    IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
    if(GetSize() == 0)  return false;

    int index = KeyIndex(key, KM);
    // cout << index << " ";
    if(index >= GetSize()) return false;

    GenericKey *target = KeyAt(index);

    if(KM.CompareKeys(key, target)){  // 非零返回，说明未出现
        return false;
    }
    else {
        value = ValueAt(index);
        return true;
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
    int index = KeyIndex(key, KM);
    int size = GetSize() - 1;

    // 从左向右开始更新leaf node
    for(int i = index; i < size; i++){
        SetKeyAt(i, KeyAt(i + 1));
        SetValueAt(i, ValueAt(i + 1));
    }

    // 更新size
    SetSize(size);
    return size;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
    recipient->CopyNFrom(PairPtrAt(0),GetSize());
    recipient->SetNextPageId(GetNextPageId());
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
    recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
    for(int i = 0; i < GetSize() - 1; i++){
        SetKeyAt(i, KeyAt(i+1));
        SetValueAt(i, ValueAt(i+1));
    }
    IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
    SetKeyAt(GetSize(), key);
    SetValueAt(GetSize(), value);
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
    int end = GetSize() - 1;
    recipient->CopyFirstFrom(KeyAt(end), ValueAt(end));
    IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
    int i;
    /* 从后向前更新key和rid，直到目的key > key[i]， 或者插入index为0 */
    for(i = GetSize(); i > 0; i--){
        SetKeyAt(i, KeyAt(i-1));
        SetValueAt(i, ValueAt(i-1));
    }

    /* 插入key和value,更新Size为size+1 */
    SetKeyAt(i, key);
    SetValueAt(i, value);
    IncreaseSize(1);
}
