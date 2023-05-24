#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
    if(Size() == 0){
        frame_id = nullptr;
        return false;
    }
    *frame_id = lru_list_.front();
    lru_table_.erase(lru_list_.front());
    lru_list_.pop_front();
    return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
    if(lru_table_.find(frame_id) != lru_table_.end()){
        lru_list_.erase(lru_table_[frame_id]);
        lru_table_.erase(frame_id);
    }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
    if(lru_table_.find(frame_id) == lru_table_.end()){
        lru_list_.push_back(frame_id);
        lru_table_[frame_id] = prev(lru_list_.end());
    }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list_.size();
}