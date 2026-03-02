// Copyright (c) Simon Fraser University & The Chinese University of Hong Kong. All rights reserved.
// Licensed under the MIT license.
#pragma once

/*
* Parent function of all hash indexes
* Used to define the interface of the hash indexes
*/

namespace dash_eh {

typedef size_t Key_t;
typedef const char* Value_t;

const Key_t SENTINEL = -2;  // 11111...110
const Key_t INVALID = -1;   // 11111...111

const Value_t NONE = 0x0;
const Value_t DEFAULT = reinterpret_cast<Value_t>(1);

/*variable length key*/
struct string_key{
    int length;
    char key[0];
};

struct Pair {
  Key_t key;
  Value_t value;

  Pair(void) : key{INVALID} {}

  Pair(Key_t _key, Value_t _value) : key{_key}, value{_value} {}

  Pair& operator=(const Pair& other) {
    key = other.key;
    value = other.value;
    return *this;
  }

  void* operator new(size_t size) {
    void* ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  void* operator new[](size_t size) {
    void* ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }
};

template <class T>
class Hash {
 public:
  Hash(void) = default;
  ~Hash(void) = default;
  /*0 means success insert, -1 means this key already exist, directory return*/
  virtual int Insert(T, Value_t) = 0;
  virtual int Insert(T, Value_t, bool) = 0;
 
  virtual void bootRestore(){

  };
  virtual void reportRestore(){

  };
  virtual bool Delete(T) = 0;
  virtual bool Delete(T, bool) = 0;
  virtual bool Get(T, Value_t*) = 0;
  virtual bool Get(T key, Value_t*, bool is_in_epoch) = 0;
  virtual void getNumber() = 0;
};

} // namespace dash_eh
