#include <signal.h>
#include <iostream>
#include <string>
#include <cstring>
#include <thread>
#include <chrono>
#include <cstddef>
#include <atomic>
#include "libipc/ipc.h"
#include "libipc/shm.h"
#include "capo/random.hpp"
#include <dlfcn.h>
#include "operation.pb.h"


#include "function_lib.hpp"
#include "yaml-cpp/yaml.h"


std::atomic<bool> is_quit__{ false };

unsigned ExecutorTimerThreshold = 1000; // every second
unsigned RecvWaitTm;
string funcDir;


ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;
// zmq::context_t context(1);

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

bool load_function(logger log, string &func_name, map<string, CppFunction> &name_func_map){
    auto start_t = std::chrono::system_clock::now();
    
    string lib_name = funcDir + func_name + ".so";
    void *lib_handle = dlopen(lib_name.c_str(), RTLD_LAZY);
    if (!lib_handle) {
        std::cerr << func_name << ".so load failed (" << dlerror() << ")" << std::endl;
        log->error("Load lib {}.so failed", func_name);
        return false;
    }

    auto lib_load_t = std::chrono::system_clock::now();
 
    char *error = 0;
    CppFunction func = (CppFunction) dlsym(lib_handle, "handle");
    if ((error = dlerror()) != NULL) {
        std::cerr << "get handle function failed (" << error << ")" << std::endl;
        log->error("Load handle function from {}.so failed: {}", func_name, error);
        return false;
    }
    auto func_load_t = std::chrono::system_clock::now();

    name_func_map[func_name] = func;
    auto lib_load_elasped = std::chrono::duration_cast<std::chrono::microseconds>(lib_load_t - start_t).count();
    auto func_load_elasped = std::chrono::duration_cast<std::chrono::microseconds>(func_load_t - lib_load_t).count();
    log->info("Loaded function {}. lib_load_elasped: {}, func_load_elasped: {}", func_name, lib_load_elasped, func_load_elasped);
    return true;
}

inline void update_status(unsigned thread_id, bool busy_flag, SocketCache & socket_cache_){
  ExecutorStatusMessage status_req;
  status_req.set_thread_id(static_cast<uint32_t>(thread_id));
  status_req.set_status (busy_flag? 6: 7);
  string serialized;
  status_req.SerializeToString(&serialized);
  send_no_block_msg(&socket_cache_["ipc:///requests/update_status"], serialized);
}


void run(Address ip, unsigned thread_id) {
  string log_file = "log_executor_" + std::to_string(thread_id) + ".txt";
  string log_name = "log_executor_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  // SocketCache socket_cache_(&context, ZMQ_PUSH);

  UserLibraryInterface *user_lib = new UserLibrary(ip, thread_id);
  map<string, CppFunction> name_func_map;

  string chan_name = "ipc-" + std::to_string(thread_id);
  local_chan = new shm_chan_t{chan_name.c_str(), ipc::receiver};
  
  std::cout << "Running executor...\n";
  log->info("Running executor...");

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  zmq::socket_t func_call_puller(context, ZMQ_PULL);
  func_call_puller.bind("ipc:///requests/func_call_" + std::to_string(thread_id));
  // func_call_puller.bind("tcp://*:" + std::to_string(thread_id + 8550));

  vector<zmq::pollitem_t> pollitems = {
    {static_cast<void *>(func_call_puller), 0, ZMQ_POLLIN, 0}
  };

  while (true){

    kZmqUtil->poll(0, &pollitems);
    if (pollitems[0].revents & ZMQ_POLLIN) {
      auto recv_stamp = std::chrono::system_clock::now();
      string serialized = kZmqUtil->recv_string(&func_call_puller);
      log->info("receive function call request");
      FunctionCallToExecutor req;
      req.ParseFromString(serialized);
      // update_status(thread_id, true, socket_cache_);
      string resp_address = req.resp_address();
      string func_name = req.func_name();

      static_cast<UserLibrary*>(user_lib)->set_function_name(func_name);
      static_cast<UserLibrary*>(user_lib)->set_resp_address(resp_address);

      if (name_func_map.find(func_name) == name_func_map.end()){
        // read .so from shared memory dir
        if(!load_function(log, func_name, name_func_map)){
          log->error("Fail to execute function {} due to load error", func_name);
          update_status(thread_id, false, socket_cache_);
          continue;
        }
      }


      int arg_size = req.args().size();
      char ** arg_values;
      arg_values = new char*[req.args().size()];
      int i = 0;
      for(auto arg: req.args()) {
        char * arg_v = new char[arg.size() + 1];
        std::copy(arg.begin(), arg.end(), arg_v);
        arg_v[arg.size()] = '\0';
        arg_values[i++] = arg_v;
        static_cast<UserLibrary*>(user_lib)->add_arg_size(arg.size() + 1);
      }

      auto recv_time = std::chrono::duration_cast<std::chrono::microseconds>(recv_stamp.time_since_epoch()).count();
      auto parse_stamp = std::chrono::system_clock::now();
      auto parse_time = std::chrono::duration_cast<std::chrono::microseconds>(parse_stamp.time_since_epoch()).count();

      log->info("Executing {} arg_size: {}. recv: {}, parse: {}", func_name, arg_size, recv_time, parse_time);

      int exit_signal = name_func_map[func_name](user_lib, arg_size, arg_values);
      if (exit_signal != 0){
        std::cerr << "Function " << func_name << " exits with error " << exit_signal << std::endl;
        log->warn("Function {} exits with error {}", func_name, exit_signal);
      }
      auto execute_stamp = std::chrono::system_clock::now();
      static_cast<UserLibrary*>(user_lib)->clear_session();

      update_status(thread_id, false, socket_cache_);

      auto execute_time = std::chrono::duration_cast<std::chrono::microseconds>(execute_stamp.time_since_epoch()).count();
      log->info("Executed {} at: {}", func_name, execute_time);
      
    }



    report_end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(report_end - report_start).count();

    if (duration >= ExecutorTimerThreshold) {
      report_start = std::chrono::system_clock::now();
      update_status(thread_id, false, socket_cache_);      
      // log->info("Executer {} report.", thread_id);
    }
   
  }
  std::cout << __func__ << ": quit...\n";
}

int main(int argc, char *argv[]) {
    auto exit = [](int) {
        is_quit__.store(true, std::memory_order_release);
        shared_chan.disconnect();
        local_chan->disconnect();
    };
    ::signal(SIGINT  , exit);
    ::signal(SIGABRT , exit);
    ::signal(SIGSEGV , exit);
    ::signal(SIGTERM , exit);
    ::signal(SIGHUP  , exit);

    // read the YAML conf
    YAML::Node conf;
    if (argc == 2){ 
      string executor_conf = "executor_" + std::string(argv[1]);
      conf = YAML::LoadFile("conf/local.yml")[executor_conf];
    }
    else conf = YAML::LoadFile("conf/config.yml");
    std::cout << "Read file config.yml" << std::endl;

    funcDir = conf["func_dir"].as<string>();

    if (YAML::Node wait_tm = conf["wait"]) {
      RecvWaitTm = wait_tm.as<unsigned>();
    } 
    else {
      RecvWaitTm = 0;
    }

    Address ip = conf["ip"].as<Address>();
    unsigned thread_id = conf["thread_id"].as<unsigned>();

    run(ip, thread_id);
}


