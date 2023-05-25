#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(GetMetaData());
  uint32_t extentNums = meta_page->GetExtentNums(),
           extentIndex = 0;
  size_t pageSize = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  bool extent_found = false;

  /* 1. 在已有extent中寻找是否有可存储的空位 */
  for(; extentIndex < extentNums; extentIndex++)
  {
    if(meta_page->GetExtentUsedPage(extentIndex) < pageSize)
    {
      extent_found = true;
      break;
    }
  }

  /* 2. 如果有可存储空位，在该extent中进行存储 */
  uint32_t ofs;
  meta_page->num_allocated_pages_++;
  page_id_t BMAddr = (pageSize + 1) * extentIndex + 1 ;
  // (pageSize+1): 包含BitMap; *extentIndex: 已有extent数-1; +1: 到当前BitMap
  if(extent_found)
  {
    char currBMap[PAGE_SIZE];
    /* 2.1. 读取该extent的bitMap */
    ReadPhysicalPage(BMAddr, currBMap);
    /* 2.2. 分配页空间 */
    reinterpret_cast<BitmapPage<PAGE_SIZE> *>(currBMap)->AllocatePage(ofs);
    /* 2.3. metaPage信息更新 */
    meta_page->extent_used_page_[extentIndex]++;
    /* 2.4. 写入新BMap */
    WritePhysicalPage(BMAddr, currBMap);
  }
  /* 3. 未找到有空位的extent，新建extent */
  else
  {
    char newBMap[PAGE_SIZE];
    memset(newBMap, 0, sizeof(newBMap));
    meta_page->num_extents_++;
    meta_page->extent_used_page_[extentIndex]++;
    reinterpret_cast<BitmapPage<PAGE_SIZE> *>(newBMap)->AllocatePage((uint32_t &)ofs);
    WritePhysicalPage(BMAddr, newBMap);
  }

  return ofs + pageSize * extentIndex;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id)
{
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(GetMetaData());
  size_t pageSize = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  char currBMap[PAGE_SIZE];
  uint32_t extentIndex = logical_page_id / pageSize,
           ofs = logical_page_id % pageSize;

  ReadPhysicalPage((pageSize + 1) *extentIndex + 1, currBMap);

  if(!reinterpret_cast<BitmapPage<PAGE_SIZE> *>(currBMap)->DeAllocatePage(ofs))
  {
    // std::cout << "Deallocate Error" << std::endl;
    return;
  }

  WritePhysicalPage((pageSize + 1) * extentIndex + 1, currBMap);

  meta_page->num_allocated_pages_--;
  meta_page->extent_used_page_[extentIndex]--;

  return;
}

/**
 * TODO: Student Implement
 * 思路类似于Allocate方法：先找extent位置，找到对应BitMap的空判断函数来做判断
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  size_t pageSize = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  char currBMap[PAGE_SIZE];
  uint32_t extentIndex = logical_page_id / pageSize,
           ofs = logical_page_id % pageSize;

  ReadPhysicalPage((pageSize + 1) *extentIndex + 1, currBMap);

  return reinterpret_cast<BitmapPage<PAGE_SIZE> *>(currBMap)->IsPageFree(ofs);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  size_t pageSize = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();

  // return (pageSize+1) * (logical_page_id / pageSize) + 1 + (logical_page_id % pageSize+1);
  //    实际extent页数 * 在该page前的extent数 + 当前extent的BMap + (当前extent的页下标+1)
  return logical_page_id / pageSize + logical_page_id + 2;
  //    bitMap数 + 数据存储页数 + 当前extent的bitMap + 1（extent存储下标从0开始）
}


int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}