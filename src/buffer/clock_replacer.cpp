//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

#include "common/logger.h"
namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages)
    : num_pages_(num_pages), clock_hand_(0), rb_(num_pages_, FrameStat()), real_size_(0) {}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (Size() == 0) {
    return false;
  }

  while (true) {
    FrameStat &frame_stat = rb_[clock_hand_];
    if (frame_stat.is_in) {
      if (frame_stat.chances > 0) {
        frame_stat.chances -= 1;
      } else {
        frame_stat.is_in = false;
        --real_size_;
        *frame_id = clock_hand_;
        return true;
      }
    }
    clock_hand_ = (clock_hand_ + 1) % num_pages_;
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  FrameStat &frame_stat = rb_[frame_id];
  if (frame_stat.is_in) {
    frame_stat.is_in = false;
    --real_size_;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  FrameStat &frame_stat = rb_[frame_id];
  if (!frame_stat.is_in) {
    frame_stat.is_in = true;
    frame_stat.chances = 1;
    ++real_size_;
  }
}

size_t ClockReplacer::Size() { return real_size_; }

}  // namespace bustub
