#ifndef INCLUDE_COORD_HANDLERS_HPP_
#define INCLUDE_COORD_HANDLERS_HPP_

#include "common.hpp"
#include "requests.hpp"
#include "types.hpp"
#include "kvs_threads.hpp"
#include "op_threads.hpp"
#include "trigger.hpp"
#include "operation.pb.h"

struct NodeStatus {
  TimePoint tp_;
  int avail_executors_;
  set<string> functions_;

  // NodeStatus(int avail_executors, set<string> &functions): avail_executors_(avail_executors), functions_(functions){
  //   tp_ = std::chrono::system_clock::now();
  // }

  void update_status(int avail_executors, set<string> &functions){
    avail_executors_ = avail_executors;
    functions_ = functions;
    tp_ = std::chrono::system_clock::now();
  }
};

struct AppInfo {
  set<string> functions_;
  map<string, set<string>> direct_deps_;
  vector<Bucket> buckets_;
};

struct GetAddressResult {
  KVSError error_;
  set<Address> addr_;

  GetAddressResult(){
    error_ = KVSError::SUCCESS;
    addr_ = {};
  }

  GetAddressResult(KVSError error): error_(error) {
    addr_ = {};
  }
};

inline string get_func_exec_address(string ip, unsigned io_thread){
  return "tcp://" + ip + ":" + std::to_string(funcExecPort + io_thread);
}

inline string get_executor_func_call_address(string ip, uint32_t executor_id){
  return "tcp://" + ip + ":" + std::to_string(executorFuncCallPort + executor_id);
}

void update_address(
    const BucketKey &bucket_key,
    const Address &addr, 
    map<Bucket, map<Key, set<Address>>> &normal_key_address_map,
    map<Bucket, map<Session, map<Key, set<Address>>>> &session_key_address_map,
    logger log);

GetAddressResult get_address(
    const BucketKey &bucket_key,
    map<Bucket, map<Key, set<Address>>> &normal_key_address_map,
    map<Bucket, map<Session, map<Key, set<Address>>>> &session_key_address_map,
    logger log);

void notify_handler(logger log, string &serialized, SocketCache &pushers,
                    map<Bucket, map<Key, set<Address>>> &normal_key_address_map,
                    map<Bucket, map<Session, map<Key, set<Address>>>> &session_key_address_map,
                    map<Bucket, ValueType> &bucket_type_map,
                    map<Bucket, vector<TriggerPointer>> &bucket_triggers_map,
                    map<Bucket, string> &bucket_app_map,
                    map<string, string> &bucket_key_val_map,
                    map<string, set<Address>> &bucket_node_map,
                    map<Address, NodeStatus> &node_status_map);

void query_handler(logger log, string &serialized, SocketCache &pushers, 
                    map<Bucket, map<Key, set<Address>>> &normal_key_address_map,
                    map<Bucket, map<Session, map<Key, set<Address>>>> &session_key_address_map,
                    map<Bucket, ValueType> &bucket_type_map);

void bucket_op_handler(logger log, string &serialized, SocketCache &pushers, 
                        map<Bucket, ValueType> &bucket_type_map,
                        map<Bucket, vector<TriggerPointer>> &bucket_triggers_map,
                        map<string, set<string>> &app_buckets_map);

void trigger_op_handler(logger log, string &serialized, string &private_ip, unsigned &thread_id, SocketCache &pushers,
                        map<Bucket, ValueType> &bucket_type_map,
                        map<Bucket, vector<TriggerPointer>> &bucket_triggers_map,
                        map<Address, NodeStatus> &node_status_map);

void func_call_handler(logger log, string &serialized, SocketCache &pushers,
                        map<Address, NodeStatus> &node_status_map, map<Address, map<uint32_t, uint32_t>> &executor_status_map);

void executor_put_handler(logger log, string &serialized, SocketCache &pushers,
                        map<Address, NodeStatus> &node_status_map, map<Address, map<uint32_t, uint32_t>> &executor_status_map,
                        map<string, AppInfo> &app_info_map, map<string, string> &key_val_map, map<string, unsigned> &key_len_map,
                        map<string, string> &func_app_map);

void app_register_handler(logger log, string &serialized, string &private_ip, unsigned &thread_id, SocketCache &pushers, 
                        map<Bucket, ValueType>              &bucket_type_map, 
                        map<Bucket, vector<TriggerPointer>> &bucket_triggers_map,
                        map<string, set<string>>            &app_buckets_map,
                        map<Bucket, string>                 &bucket_app_map,
                        map<Address, NodeStatus>            &node_status_map,
                        map<string, AppInfo>                &app_info_map,
                        map<string, string>                 &func_app_map);
#endif // INCLUDE_COORD_HANDLERS_HPP_
