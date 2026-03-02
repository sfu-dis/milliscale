#pragma once

#include <string>
#include <unordered_map>
#include "sm-common.h"
#include "sm-oid.h"

namespace ermia {

struct UnorderedIndex;

struct TableDescriptor {
  // Map table name to descriptors, global, no CC
  static std::unordered_map<std::string, TableDescriptor*> name_map;

  // Map FID to descriptors, global, no CC
  static std::unordered_map<FID, TableDescriptor*> fid_map;

  // Map index name to OrderedIndex (primary or secondary), global, no CC
  static std::unordered_map<std::string, UnorderedIndex*> index_map;

  static inline UnorderedIndex *GetIndex(const std::string &name) {
    return index_map[name];
  }
  static inline bool NameExists(std::string name) {
    return name_map.find(name) != name_map.end();
  }
  static inline bool FidExists(FID fid) {
    return fid_map.find(fid) != fid_map.end();
  }
  static inline TableDescriptor* Get(std::string name) {
    return name_map[name];
  }
  static inline TableDescriptor* Get(FID fid) { return fid_map[fid]; }
  static inline UnorderedIndex* GetPrimaryIndex(const std::string& name) {
    return name_map[name]->GetPrimaryIndex();
  }
  static inline UnorderedIndex* GetPrimaryIndex(FID fid) {
    return fid_map[fid]->GetPrimaryIndex();
  }
  static inline TableDescriptor* New(std::string name) {
    name_map[name] = new TableDescriptor(name);
    return name_map[name];
  }
  static inline uint32_t NumTables() { return name_map.size(); }

  std::string name;
  UnorderedIndex *primary_index;
  std::vector<UnorderedIndex *> sec_indexes;

  FID tuple_fid;
  oid_array* tuple_array;

  // An auxiliary array: on primary this is the key array
  FID aux_fid_;
  oid_array* aux_array_;

  TableDescriptor(std::string& name);

  void Initialize();
  void SetPrimaryIndex(UnorderedIndex *index, const std::string &name);
  void AddSecondaryIndex(UnorderedIndex *index, const std::string &name);
  void Recover(FID tuple_fid, FID key_fid, OID himark = 0);
  inline std::string& GetName() { return name; }
  inline UnorderedIndex* GetPrimaryIndex() { return primary_index; }
  inline FID GetTupleFid() { return tuple_fid; }
  inline FID GetKeyFid() {
    return aux_fid_;
  }
  inline oid_array* GetKeyArray() {
    return aux_array_;
  }
  inline FID GetPersistentAddressFid() {
    return aux_fid_;
  }
  inline oid_array* GetPersistentAddressArray() {
    return aux_array_;
  }
  inline oid_array* GetTupleArray() { return tuple_array; }
};
}  // namespace ermia
