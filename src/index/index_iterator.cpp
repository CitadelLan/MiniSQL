#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
    return page->GetItem(item_index);
}

IndexIterator &IndexIterator::operator++() {
    if (item_index == page->GetSize() - 1){   // 页的最后一个
        item_index = 0;
        page_id_t next_page_id = page -> GetNextPageId();
        if (next_page_id!=INVALID_PAGE_ID){     // 还有下一页
            // page变成下一页
            Page* next_page = buffer_pool_manager->FetchPage(next_page_id);
            page = reinterpret_cast<LeafPage *>(next_page -> GetData());
            buffer_pool_manager->UnpinPage(next_page_id, false);
        } else{
            // page满 变成-1
            item_index--;
        }
    }
    else{
        item_index++;
    }
    return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
    return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
    return !(*this == itr);
}
