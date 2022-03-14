#ifndef INCLUDE_EXECUTOR_FUNCTION_LIB_HPP_
#define INCLUDE_EXECUTOR_FUNCTION_LIB_HPP_

#include "common.hpp"
#include "requests.hpp"
#include "kvs_threads.hpp"
#include "types.hpp"
#include "trigger.hpp"
#include "operation.pb.h"
#include "cpp_function.hpp"
#include "comm_helper.hpp"


constexpr char const kvs_name__  [] = "ipc-kvs";

typedef int (*CppFunction)(UserLibraryInterface*, int, char**);

class EpheObjectImpl : public EpheObject {
  public:
    EpheObjectImpl(string bucket, string key, size_t size, bool create) {
      obj_name_ = bucket + kDelimiter  + key;
      size_ = size;
      shm_id_ = 0;
      value_ = static_cast<void *> (new char[size_ + 1]);
      target_func_ = "";
    }

    EpheObjectImpl(string src_function, string tgt_function, size_t size): EpheObjectImpl("b_" +  tgt_function, "k_" + src_function, size, true) {
      target_func_ = tgt_function;
    }

    EpheObjectImpl(const EpheObjectImpl& other){
      obj_name_ = other.obj_name_;
      size_ = other.size_;
      value_ = other.value_;
      target_func_ = other.target_func_;
    }

    ~EpheObjectImpl(){
      ipc::shm::release(shm_id_);
    }

    void* get_value(){
      return value_;
    }

    void set_value(const void* value, size_t val_size){
      memcpy(value_, value, val_size);
    }

    void update_size(size_t size){
      size_ = size;
    }

    size_t get_size(){
      return size_;
    }

  public:
    string obj_name_;
    size_t size_;
    string target_func_;

  private:
    void* value_;
    ipc::shm::id_t shm_id_;
};

class UserLibrary : public UserLibraryInterface {
  public:
    UserLibrary(string ip, unsigned thread_id, CommHelperInterface *helper) {
      ip_number_ = ip;
      ip_number_.erase(std::remove(ip_number_.begin(), ip_number_.end(), '.'), ip_number_.end());
      chan_id_ = static_cast<uint8_t>(thread_id + 1);
      rid_ = 0;
      object_id_ = 0;
      helper_ = helper;
    }

    ~UserLibrary() {}

  public:
    void set_function_name(string &function){
      function_ = function;
    }

    void set_resp_address(string &resp_address){
      resp_address_ = resp_address;
    }

    void clear_session(){
      function_ = emptyString;
      resp_address_ = emptyString;
      size_of_args_.clear();
    }

    void add_arg_size(size_t size){
      size_of_args_.push_back(size);
    }

    size_t get_size_of_arg(int arg_idx){
      if (size_of_args_.size() <= arg_idx) return -1;
      return size_of_args_[arg_idx];
    }

    EpheObject* create_object(string bucket, string key, size_t size = 1024 * 1024) {
      return new EpheObjectImpl(bucket, key, size, true);
    }

    EpheObject* create_object(size_t size = 1024 * 1024) {
      string key = std::to_string(chan_id_) + "_" + std::to_string(get_object_id());
      return create_object(bucketNameDirectInvoc, key, size);
    }

    EpheObject* create_object(string target_function, bool many_to_one_trigger = true, size_t size = 1024 * 1024) {
      if (many_to_one_trigger) {
        return new EpheObjectImpl(function_, target_function, size);
      }
      else {
        return create_object("b_" + target_function, gen_unique_key(), size);
      }
    }

    void send_object(EpheObject *data, bool output = false, bool to_anna = false) {
      ExecutorPutMessage msg;
      if (output) {
        msg.set_option(4);
      } else {
        msg.set_option(2);
      }
      msg.set_request_id(std::to_string(get_request_id()));
      msg.set_resp_address(resp_address_);
      bool if_anna = (data->get_size() > 100);
      msg.set_if_anna(if_anna);
      msg.set_func_name(function_);
      msg.set_tgt_function(static_cast<EpheObjectImpl*>(data)->target_func_);
      msg.set_object_name(static_cast<EpheObjectImpl*>(data)->obj_name_);
      if (if_anna){
        msg.set_data(static_cast<EpheObjectImpl*>(data)->obj_name_);
        // send put to anna
        string rid = kvs_clients[0]->put_async(static_cast<EpheObjectImpl*>(data)->obj_name_, 
                                              serialize(generate_timestamp(0), 
                                              string{static_cast<char*>(data->get_value()), data->get_size()}), 
                                              LatticeType::LWW);
        // wait resp
        while(1){
          auto comm_resps = helper_->try_recv_msg();
          int recv_resp_from_anna = 0;
          for (auto &comm_resp : comm_resps) {
            if (comm_resp.msg_type_ == RecvMsgType::KvsPutResp) {
              recv_resp_from_anna = 1;
            }
          }
          if (recv_resp_from_anna) break;

        }
        
      } else
        msg.set_data(static_cast<char*>(data->get_value()), data->get_size());
      string serialized;
      msg.SerializeToString(&serialized);
      send_request(msg, static_cast<CommHelper *> (helper_)->socket_cache_[static_cast<CommHelper *>(helper_)->routing_threads_[0].executor_put_connect_address()]);

    }

    EpheObject* get_object(string bucket, string key, bool from_ephe_store=true) {
      // TODO::
      return new EpheObjectImpl("bucket", "key", 0, false);
    }

    string gen_unique_key(){
      return ip_number_ + "_" + std::to_string(chan_id_) + "_" + std::to_string(get_object_id());
    }

  private:
    uint8_t get_request_id() {
      if (++rid_ % 100 == 0) {
        rid_ = 0;
      }
      return rid_;
    }

    unsigned get_object_id() {
      return object_id_++;
    }

  private:
    string ip_number_;
    uint8_t chan_id_;
    uint8_t rid_;
    unsigned object_id_;
    string function_;
    string resp_address_;
    vector<size_t> size_of_args_;
    CommHelperInterface *helper_;
};

#endif  // INCLUDE_EXECUTOR_FUNCTION_LIB_HPP_

