#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.push_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
    frame_id_t frameId;

    /* 1.If P does not exist, find a replacement page (R) from either the free list or the replacer.
     **  Note that pages are always found from the free list first.  */
    if(page_table_.find(page_id) == page_table_.end()){
        if(!free_list_.empty()){
            frameId = free_list_.front();
            free_list_.pop_front();
        }
        else{
            bool is_replacer = replacer_->Victim(&frameId);
            cout << is_replacer;
            if(!is_replacer) return nullptr;
        }

        // 1.Search the page table for the requested page (P).
        Page *fetch_page = pages_ + frameId;
        // 2.If R is dirty, write it back to the disk.
        if(fetch_page->IsDirty())
            disk_manager_->WritePage(fetch_page->page_id_, fetch_page->GetData());

        // 3.Delete R from the page table and insert P.
        page_table_.erase(fetch_page->page_id_);
        page_table_[page_id] = frameId;

        // 4.Update P's metadata, read in the page content from disk, and then return a pointer to P.
        fetch_page->pin_count_ ++;
        fetch_page->page_id_  = page_id;
        fetch_page->is_dirty_ = false;
        disk_manager_->ReadPage(page_id, fetch_page->data_);
        replacer_->Pin(frameId);
        return fetch_page;
    }

    /* 2.If P exists, pin it and return it immediately. */
    else{
        frameId = page_table_[page_id];
        Page *fetch_page = pages_ + frameId;
        fetch_page->pin_count_ ++;
        replacer_->Pin(frameId);
        return fetch_page;
    }
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
    /* 1.If all the pages in the buffer pool are pinned, return nullptr. */
    size_t i;
    for(i = 0; i < pool_size_; i++){
        if(pages_[i].pin_count_ == 0) break;
    }
    if(i == pool_size_) return nullptr;

    /* 2.Pick a victim page from either the free list or the replacer. Always pick from the free list first. */
    frame_id_t victim_frameId;
    if(!free_list_.empty()){
        victim_frameId = free_list_.front();
        free_list_.pop_front();
    }
    else{
        bool is_replacer = replacer_->Victim(&victim_frameId);
        if(!is_replacer) return nullptr;
    }

    /* 3.Set the page ID output parameter. Update P's metadata, zero out memory and add P to the page table. */
    page_id = AllocatePage();
    Page *new_page = pages_ + victim_frameId;
    if(new_page->IsDirty()){
        new_page->is_dirty_ = false;
        disk_manager_->WritePage(new_page->GetPageId(), EMPTY_PAGE_DATA);
    }

    page_table_.erase(new_page->page_id_);
    page_table_[page_id] = victim_frameId;
    new_page->ResetMemory();
    new_page->is_dirty_ = false;
    new_page->page_id_  = page_id;
    new_page->pin_count_++;

    return new_page;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
    // 0.Make sure you call DeallocatePage!
    DeallocatePage(page_id);

    // 1.If P does not exist, return true.
    if(page_table_.find(page_id) == page_table_.end())
        return true;

    else{
        // 1.Search the page table for the requested page (P).
        frame_id_t del_frameId = page_table_[page_id];
        Page *del_page = pages_ + del_frameId;
        // 2.If P exists, but has a non-zero pin-count, return false. Someone is using the page.
        if(del_page->pin_count_ != 0) return false;

        // 3.Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
        page_table_.erase(page_id);
        del_page->pin_count_ = 0;
        del_page->is_dirty_ = false;
        del_page->page_id_ = INVALID_PAGE_ID;
        free_list_.push_back(del_frameId);
        return true;
    }
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    /* 1.判断该页是否在buffer中 */
    if(page_table_.find(page_id) == page_table_.end())
        return true;

    /* 2.找到该页所在位置并修改is_dirty */
    frame_id_t frameId = page_table_[page_id];
    Page *unpin_page = pages_ + frameId;
    if(!unpin_page->IsDirty())              // 若之前is_dirty为true不做修改
        unpin_page->is_dirty_ = is_dirty;

    /* 3.判断是否为最后pinned */
    if(unpin_page->pin_count_ == 0 )
        return false;
    else if(--unpin_page->pin_count_ == 0)
        replacer_->Unpin(frameId);
    return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
    /* 1.判断该页是否在buffer中 */
    if(page_table_.find(page_id) == page_table_.end())
        return false;

    /* 2.找到该页并写进磁盘 */
    Page *flush_page = pages_ + page_table_[page_id];
    disk_manager_->WritePage(page_id, flush_page->GetData());

    /* 3.更新is_dirty */
    flush_page->is_dirty_ = false;
    return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}