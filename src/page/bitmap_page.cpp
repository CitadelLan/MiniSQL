#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  /* 判断该位置是否有bitmap映射（该bit是否为0）*/
  page_offset = next_free_page_;
  if (IsPageFree(page_offset)) {
    /* 用位或方法添加1，总page数+1 */
    bytes[page_offset / 8] |= 1 << (page_offset % 8);
    page_allocated_++;

    /* 找新的free_page——暂定线性搜索 */
    for(int i = 0; i < MAX_CHARS*8 && page_allocated_ < MAX_CHARS*8; i++)
    {
      if(IsPageFree(i)) {
        next_free_page_ = i;
        break;
      }
    }

    return true;
  }

  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  /* 判断该位置是否有bitmap映射（该bit是否为1） */
  if (!IsPageFree(page_offset)) {
    /* 用减方法（也可以用位与，但好像测试有问题）添加0，总page数-1 */
    bytes[page_offset / 8] -= (1 << (page_offset%8));
    page_allocated_--;

    /* 找新的free_page，可以用当前page来做 */
    next_free_page_ = page_offset;

    return true;
  }

  return false;
}

/**
 * TODO: Student Implement
 * 对应位为 1 时，表明对应的页存在，反之亦然。
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

/**
 * bitmap关键：判断bitmap对应位是否是1
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (((bytes[byte_index] >> bit_index) & 1) == 0);
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;