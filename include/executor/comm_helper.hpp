#ifndef INCLUDE_KVS_HELPER_HPP_
#define INCLUDE_KVS_HELPER_HPP_

#include "common.hpp"
#include "anna_client/kvs_client.hpp"
#include "requests.hpp"
#include "kvs_threads.hpp"
#include "types.hpp"
#include "trigger.hpp"
#include "operation.pb.h"
#include <queue>
#include <mutex>
#include <condition_variable>
#include "safe_ptr.hpp"

enum RecvMsgType {Null, Call, KvsPutResp, KvsGetResp};
struct RecvMsg {
  RecvMsgType msg_type_;
  string app_name_;
  string resp_address_;
  vector<string> func_name_;
  vector<vector<string>> func_args_;
  vector<vector<int>> func_arg_flags_;
  string data_key_;
  string data_;
  unsigned data_size_;
};


using CommRespQueue = sf::safe_ptr<queue<RecvMsg>>;
vector<CommRespQueue> resp_queues;

vector<KvsClient*> kvs_clients;
int msg_process_batch = 10;

ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;
zmq::context_t context(8);

inline bool send_no_block_msg(zmq::socket_t* socket, zmq::message_t& message){
  try{
    socket->send(message, ZMQ_DONTWAIT);
    return true;
  }
  catch(std::exception& e){
    // std::cout << "Queue this message when error occurs " << e.what() << std::endl;
    return false;
  }
}

inline bool send_no_block_msg(zmq::socket_t* socket, const string& s){
  zmq::message_t msg(s.size());
  memcpy(msg.data(), s.c_str(), s.size());
  return send_no_block_msg(socket, msg);
}


class CommHelperInterface {
 public:
  virtual void update_status(unsigned thread_id, bool available) = 0;
  virtual void set_logger(logger log) = 0;
  virtual vector<RecvMsg> try_recv_msg() = 0;

};

class CommHelper : public CommHelperInterface {
 public:
  CommHelper(vector<HandlerThread> routing_threads, string ip, int thread_id, unsigned timeout = 10000):
      routing_threads_(routing_threads),
      ip_(ip),
      ut_(ip, 0),
      socket_cache_(SocketCache(&context, ZMQ_PUSH)),
      func_exec_puller_(zmq::socket_t(context, ZMQ_PULL)),
      timeout_(timeout) {
    seed_ = time(NULL);
    // seed_ += hasher(ip);
    // set the request ID to 0
    func_exec_puller_.bind(kBindBase + std::to_string(executorFuncCallPort + thread_id));

    zmq_pollitems_ = {
        {static_cast<void*>(func_exec_puller_), 0, ZMQ_POLLIN, 0}
    };

    rid_ = 0;
    
  }

  ~CommHelper() { 
  }

 public:

  void update_status(unsigned thread_id, bool busy) {
    UpdateExecutorStatus msg;
    msg.set_ip(ip_);
    msg.set_thread_id(static_cast<uint32_t>(thread_id));
    msg.set_busy (busy ? 1 : 0);
    string serialized;
    msg.SerializeToString(&serialized);

    for (auto &ht : routing_threads_){
      send_no_block_msg(&socket_cache_[ht.executor_update_connect_address()], serialized); 
    }

  }

  void get_kvs_responses(vector<RecvMsg> &comm_resps, unsigned tid=0){
    vector<KeyResponse> responses = kvs_clients[tid]->receive_async();
    for (auto &response : responses){
      Key key = response.tuples(0).key();

      if (response.error() == AnnaError::TIMEOUT) {
        log_->info("Kvs request io_thread {} for key {} timed out.", tid, key);
        if (response.type() == RequestType::GET) {
          kvs_clients[tid]->get_async(key);
        } 
        else {
          // TODO re-issue put request
        }
      } 
      else {
        log_->info("Thread {} Kvs response type {} error code {}", tid, response.type(), response.tuples(0).error());
        if (response.type() == RequestType::GET && response.tuples(0).error() != 1) {
          auto kvs_recv_stamp = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count();
          RecvMsg resp;
          resp.msg_type_ = RecvMsgType::KvsGetResp;

          LWWPairLattice<string> lww_lattice = deserialize_lww(response.tuples(0).payload());

          resp.data_key_ = key;
          resp.data_size_ = lww_lattice.reveal().value.size();
          string local_key_name = kvsKeyPrefix + "|" + key;
          
          // copy_func_(local_key_name, lww_lattice.reveal().value.c_str(), resp.data_size_);
          resp.data_ = string(lww_lattice.reveal().value.c_str(), resp.data_size_);
          comm_resps.push_back(resp);

          auto kvs_get_stamp = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count();
          log_->info("Kvs GET response io_thread {}. recv: {}, copy: {}.", tid, kvs_recv_stamp, kvs_get_stamp);
        } 
        else if (response.type() == RequestType::PUT){
          auto kvs_recv_stamp = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count();
          RecvMsg resp;
          resp.msg_type_ = RecvMsgType::KvsPutResp;
          resp.data_key_ = key;
          comm_resps.push_back(resp);
          log_->info("Kvs PUT response io_thread {}. recv: {}.", tid, kvs_recv_stamp);
        }
      }
    }
  }

  vector<RecvMsg> try_recv_msg(){
    kZmqUtil->poll(0, &zmq_pollitems_);
    vector<RecvMsg> comm_resps;
    if (zmq_pollitems_[0].revents & ZMQ_POLLIN) {
      auto receive_call_stamp = std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count();
      string serialized = kZmqUtil->recv_string(&func_exec_puller_);
      RecvMsg resp;

      FunctionCall call;
      call.ParseFromString(serialized);
      resp.app_name_ = call.app_name();
      resp.msg_type_ = RecvMsgType::Call;
      resp.resp_address_ = call.resp_address();
      for (auto &req : call.requests()){
        resp.func_name_.push_back(req.name());
        vector<string> args;
        vector<int> flags;
        for (auto &arg : req.arguments()){
          args.push_back(arg.body());
          flags.push_back(arg.arg_flag());
        }
        resp.func_args_.push_back(args);
        resp.func_arg_flags_.push_back(flags);
      }
      comm_resps.push_back(resp);
      string name_to_log = resp.func_name_.size() == 1 ? resp.func_name_[0] : resp.app_name_;
      log_->info("Function call {} io_thread 0. num: {}, recv: {}.", name_to_log, resp.func_name_.size(), receive_call_stamp);
    } 

    get_kvs_responses(comm_resps);

    for (auto &resp_queue: resp_queues){
      while (!resp_queue->empty()) {
        // comm_resps.push_back(resp_queue->pop_front());
        comm_resps.push_back(resp_queue->front());
        resp_queue->pop();
      }
    }
    return comm_resps;
  }

  /**
   * Set the logger used by the client.
   */
  void set_logger(logger log) { log_ = log; }


  /**
   * Return the random seed used by this client.
   */
  unsigned get_seed() { return seed_; }

 private:


 public:
  
  // class logger
  logger log_;
  // the set of routing addresses outside the cluster
  vector<HandlerThread> routing_threads_;
  SocketCache socket_cache_;

 private:
  
  zmq::socket_t func_exec_puller_;
  vector<zmq::pollitem_t> pollitems_;
  vector<zmq::pollitem_t> zmq_pollitems_;


  // the current request id
  unsigned rid_;

  // the random seed for this client
  unsigned seed_;

  // the IP and port functions for this thread
  CommHelperThread ut_;

  string ip_;

  // GC timeout
  unsigned timeout_;
};



#endif  // INCLUDE_KVS_HELPER_HPP_

