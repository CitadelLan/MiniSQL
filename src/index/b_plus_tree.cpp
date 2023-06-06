#include "index/b_plus_tree.h"

#include <string>
#include <algorithm>
#include <stack>
#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM)
{
    auto root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID));
    if (!root_page->GetRootId(index_id, &this->root_page_id_))
    {
      this->root_page_id_ = INVALID_PAGE_ID;
    }
    buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE)/(KM.GetKeySize() + sizeof(RowId)) - 1;
    internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(page_id_t)) - 1;
}

void BPlusTree::ClrDeletePages()
{
    for (int delete_page : delete_pages)
        buffer_pool_manager_->DeletePage(delete_page);

    delete_pages.clear();
}

 void BPlusTree::Destroy(page_id_t current_page_id) {
    ClrDeletePages();
 }

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID)
  {
    return true;
  }
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  if(IsEmpty()) return false;

  Page* leaf_page = this->FindLeafPage(key);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  if(!leaf_node) return false;

  RowId tmp_value;
  if(!leaf_node->Lookup(key, tmp_value, processor_))
  {
    //leaf_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  else 
  {
    //leaf_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    result.push_back(tmp_value);
    return true;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, RowId &value, Transaction *transaction) {
  if (IsEmpty()) 
  {
    StartNewTree(key, value);
    return true;
  }

  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  Page* root_page = buffer_pool_manager_->NewPage(root_page_id_);

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(root_page->GetData());
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  leaf_page->Insert(key, value, processor_);

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);

  UpdateRootPageId(1);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, RowId &value, Transaction *transaction) {
  Page* find_leaf_page = this->FindLeafPage(key);
  LeafPage* tmp_leaf_page = reinterpret_cast<LeafPage*>(find_leaf_page->GetData());

  /* 检查是否是重复键 */
  if(tmp_leaf_page->Lookup(key, value, processor_))
  {
    buffer_pool_manager_->UnpinPage(find_leaf_page->GetPageId(), false);
    return false;
  }

  /* 检查插入是否溢出 */
  int new_size = tmp_leaf_page->Insert(key, value, processor_);

  if (new_size < leaf_max_size_)
  {
    // find_leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(find_leaf_page->GetPageId(), true);
    return true;
  }
  else
  {
    auto sibling_leaf_node = Split(tmp_leaf_page, transaction);
    sibling_leaf_node->SetNextPageId(tmp_leaf_page->GetNextPageId());
    tmp_leaf_page->SetNextPageId(sibling_leaf_node->GetPageId());
    GenericKey* risen_key = sibling_leaf_node->KeyAt(0);
    InsertIntoParent(tmp_leaf_page, risen_key, sibling_leaf_node, transaction);
    // find_leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(find_leaf_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_leaf_node->GetPageId(), true);
    return true;
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  InternalPage* new_node = reinterpret_cast<InternalPage*>(new_page->GetData());
  new_node->SetPageType(IndexPageType::INTERNAL_PAGE);
  InternalPage *internal = reinterpret_cast<InternalPage *>(node);
  InternalPage *new_internal = reinterpret_cast<InternalPage *>(new_node);
  new_internal->Init(new_page->GetPageId(), node->GetParentPageId(), processor_.GetKeySize());
  internal->MoveHalfTo(new_internal, buffer_pool_manager_);
  return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  LeafPage* new_node = reinterpret_cast<LeafPage*>(new_page->GetData());
  new_node->SetPageType(IndexPageType::LEAF_PAGE);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(node);
  LeafPage *new_leaf = reinterpret_cast<LeafPage *>(new_node);
  new_leaf->Init(new_page->GetPageId(), node->GetParentPageId(), leaf->GetKeySize());
  leaf->MoveHalfTo(new_leaf);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  if (old_node->IsRootPage()) 
  {
    Page* new_page = buffer_pool_manager_->NewPage(root_page_id_);
    InternalPage *new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize());
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    UpdateRootPageId(0);
    return;
  }

  Page* parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int new_size = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  if (new_size < internal_max_size_)
  {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }
  auto parent_new_sibling_node = Split(parent_node, transaction);
  GenericKey* new_key = parent_new_sibling_node->KeyAt(0);
  InsertIntoParent(parent_node, new_key, parent_new_sibling_node, transaction);

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_new_sibling_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  if (IsEmpty()) return;

  Page* leaf_page = FindLeafPage(key);
  LeafPage *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int org_size = node->GetSize();

  if (org_size != node->RemoveAndDeleteRecord(key, processor_))
  {
    // leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }
  else 
  {
    // leaf_page->WUnlatch();
    bool if_deletion_occur = CoalesceOrRedistribute(node, transaction);
    if(if_deletion_occur)
    {
      delete_pages.push_back(node->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    ClrDeletePages();
  }
  return;
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  if (node->IsRootPage()) 
  {
    bool if_delete_root = AdjustRoot(node);
    return if_delete_root;
  }
  else if (node->GetSize() >= node->GetMinSize()) 
  {
    return false;
  }
  else 
  {}

  Page* fth_parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* tmp_parent_page = reinterpret_cast<InternalPage *>(fth_parent_page->GetData());
  int index = tmp_parent_page->ValueIndex(node->GetPageId());
  int r_index;
  if(index == 0)
    r_index = 1;
  else 
    r_index = index -1;

  Page* sibling_page = buffer_pool_manager_->FetchPage(
    tmp_parent_page->ValueAt(r_index));
  N* sibling_node = reinterpret_cast<N*>(sibling_page->GetData());

  if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize()) 
  {
    Redistribute(sibling_node, node, index);
    buffer_pool_manager_->UnpinPage(tmp_parent_page->GetPageId(), true);
    // sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false;
  }
  else 
  {
    bool if_should_delete = Coalesce(sibling_node, node, tmp_parent_page, index);
    if(if_should_delete)
    {
      delete_pages.push_back(tmp_parent_page->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(tmp_parent_page->GetPageId(), true);
    // sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return true;
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
    if(index == 0) { swap(node, neighbor_node); }
    node ->MoveAllTo(neighbor_node);
    parent->Remove(parent->ValueIndex(node->GetPageId()));
    if (parent->GetSize() < parent->GetMinSize()) return true;
    else return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
    if (index == 0) { swap(node, neighbor_node); }
    auto *temp_page = reinterpret_cast<LeafPage *>(FindLeafPage(nullptr, node->GetPageId(), true));
    GenericKey *key = temp_page->KeyAt(0);
    buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false);
    node->MoveAllTo(neighbor_node, key, buffer_pool_manager_);
    parent->Remove(parent->ValueIndex(node->GetPageId()));
    if (parent->GetSize() < parent->GetMinSize()) return true;
    else return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
    if (index == 0) {
        neighbor_node->MoveFirstToEndOf(node);
    } else {
        neighbor_node->MoveLastToFrontOf(node);
    }
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
    if (index == 0) {
        auto *temp_page = reinterpret_cast<LeafPage *>(FindLeafPage(nullptr, neighbor_node->GetPageId(), true));
        GenericKey *key = temp_page->KeyAt(0);
        buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false);
        neighbor_node->MoveFirstToEndOf(node, key, buffer_pool_manager_);
    } else {
        auto *temp_page = reinterpret_cast<LeafPage *>(FindLeafPage(nullptr, node->GetPageId(), true));
        GenericKey *key = temp_page->KeyAt(0);
        buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false);
        neighbor_node->MoveLastToFrontOf(node, key, buffer_pool_manager_);
    }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
    if (old_root_node->GetSize() == 0) {
        auto index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
        index_root_page->Delete(index_id_);
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
        return true;
    } else if (old_root_node->GetSize() == 1) {
        root_page_id_ = reinterpret_cast<InternalPage *>(old_root_node)->RemoveAndReturnOnlyChild();
        auto new_root_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(root_page_id_));
        new_root_page->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        UpdateRootPageId(0);         // 默认为false
        return true;
    } else {
        return false;
    }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
    if(IsEmpty()) return IndexIterator();
    Page* first_page = FindLeafPage(nullptr ,root_page_id_ ,true);
    page_id_t page_id = first_page->GetPageId();
    return IndexIterator(page_id, buffer_pool_manager_);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
    if (IsEmpty()) return IndexIterator();
    auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_, false));
    buffer_pool_manager_ ->UnpinPage(leaf_page->GetPageId(), false);
    int index = leaf_page->KeyIndex(key, processor_);
    if(index == -1){
        return IndexIterator();
    } else{
        return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, index);
    }
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
    /* 找到最左侧叶节点 */
    Page *leftistPg = FindLeafPage(nullptr, root_page_id_, true);
    LeafPage *currLeaf = reinterpret_cast<LeafPage *>(leftistPg->GetData());

    /* 找到最右侧叶节点 */
    while(currLeaf->GetNextPageId() != INVALID_PAGE_ID)
    {
      Page *nextPg = buffer_pool_manager_->FetchPage(currLeaf->GetNextPageId());
      auto nextLeaf = reinterpret_cast<LeafPage *>(nextPg->GetData());
      buffer_pool_manager_->UnpinPage(currLeaf->GetPageId(), false);
      currLeaf = nextLeaf;
    }

    return IndexIterator(currLeaf->GetPageId(), buffer_pool_manager_, currLeaf->GetSize() - 1);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
    if(IsEmpty()) return nullptr;
    page_id_t next_page_id = page_id;
    if(page_id == INVALID_PAGE_ID) next_page_id = root_page_id_;
    auto page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page_id));
    // 根节点被pin

    while (!page->IsLeafPage()){
      buffer_pool_manager_ ->UnpinPage(next_page_id, false);
      // Unpin当前层
      next_page_id = leftMost ? page->ValueAt(0)
                              : page->Lookup(key, processor_);
      page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page_id));
      // pin下一层
    }

    buffer_pool_manager_->UnpinPage(next_page_id, false);

    return reinterpret_cast<Page *>(page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
    auto root_index_page =
            reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_ -> FetchPage(INDEX_ROOTS_PAGE_ID));

    /* 如果 insert_record 为false， Update，否则 Insert */
    if(!insert_record) {
        root_index_page->Update(index_id_, root_page_id_);
    } else{
        root_index_page->Insert(index_id_,root_page_id_);
    }
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}
