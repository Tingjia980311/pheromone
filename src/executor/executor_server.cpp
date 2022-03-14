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
#include "comm_helper.hpp"
#include "anna_client/kvs_client.hpp"
#include "function_lib.hpp"
#include "yaml-cpp/yaml.h"


std::atomic<bool> is_quit__{ false };

unsigned ExecutorTimerThreshold = 1000; // every second
unsigned RecvWaitTm;
string funcDir;

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


void run(CommHelperInterface *helper, Address ip, unsigned thread_id, logger log) {
  

  UserLibraryInterface *user_lib = new UserLibrary(ip, thread_id, helper);
  map<string, CppFunction> name_func_map;

  std::cout << "Running executor...\n";
  log->info("Running executor...");

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  while (true){
    auto comm_resps = helper->try_recv_msg();

    for (auto &comm_resp : comm_resps) {
      if (comm_resp.msg_type_ == RecvMsgType::Call) {
        for (int i = 0; i < comm_resp.func_name_.size(); i++){
          helper->update_status(thread_id, true);
          auto func_name = comm_resp.func_name_[i];
          auto func_args = comm_resp.func_args_[i];
          // auto arg_flag = comm_resp.is_func_arg_keys_[i];
          auto resp_address = comm_resp.resp_address_;
          static_cast<UserLibrary*>(user_lib)->set_function_name(func_name);
          static_cast<UserLibrary*>(user_lib)->set_resp_address(resp_address);
          if (name_func_map.find(func_name) == name_func_map.end()){
          // read .so from shared memory dir
            if(!load_function(log, func_name, name_func_map)){
              log->error("Fail to execute function {} due to load error", func_name);
              continue;
            }
          }
          int arg_size;
          char ** arg_values;

          arg_size = comm_resp.func_args_[i].size();
          arg_values = new char*[arg_size];
          for (int j = 0; j < arg_size; j++){
            if (comm_resp.func_arg_flags_[i][j] == 0) {
              auto arg_size_in_bytes = comm_resp.func_args_[i][j].size();
              char * arg_v = new char[arg_size_in_bytes + 1];
              std::copy(comm_resp.func_args_[i][j].begin(), comm_resp.func_args_[i][j].end(), arg_v);
              arg_v[arg_size_in_bytes] = '\0';
              arg_values[i] = arg_v;
              static_cast<UserLibrary*>(user_lib)->add_arg_size(arg_size_in_bytes);
            } else {
              
              kvs_clients[0]->get_async(comm_resp.func_args_[i][j]);
              while(1){
                auto comm_resps = helper->try_recv_msg();
                int recv_resp_from_anna = 0;
                for (auto &comm_resp : comm_resps) {
                  if (comm_resp.msg_type_ == RecvMsgType::KvsGetResp) {
                    auto arg_size_in_bytes = comm_resp.data_.size();
                    char * arg_v = new char[arg_size_in_bytes + 1];
                    std::copy(comm_resp.data_.begin(), comm_resp.data_.end(), arg_v);
                    arg_v[arg_size_in_bytes] = '\0';
                    arg_values[i] = arg_v;
                    static_cast<UserLibrary*>(user_lib)->add_arg_size(arg_size_in_bytes);
                    recv_resp_from_anna = 1;
                    break;
                  }
                }
                if (recv_resp_from_anna) break;

              }
              
            }
          }
          
          int exit_signal = name_func_map[func_name](user_lib, arg_size, arg_values);
          if (exit_signal != 0){
            std::cerr << "Function " << func_name << " exits with error " << exit_signal << std::endl;
            log->warn("Function {} exits with error {}", func_name, exit_signal);
          }
          static_cast<UserLibrary*>(user_lib)->clear_session();
          helper->update_status(thread_id, false);
          log->info("Executed {}", func_name);
        
        }
      }
    }
    report_end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(report_end - report_start).count();
    

    if (duration >= ExecutorTimerThreshold) {
      report_start = std::chrono::system_clock::now();
      // send to coordinator
      helper->update_status(thread_id, false);
      // log->info("Executer {} report.", thread_id);
    }
   
  }
  std::cout << __func__ << ": quit...\n";
}

int main(int argc, char *argv[]) {
    auto exit = [](int) {
        is_quit__.store(true, std::memory_order_release);
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

    YAML::Node user = conf["user"];
    Address ip = user["ip"].as<Address>();
    unsigned thread_id = user["thread_id"].as<unsigned>();

    unsigned coordThreadCount = conf["threads"]["coord"].as<unsigned>();

    vector<Address> coord_ips;
    YAML::Node coord = user["coord"];
    for (const YAML::Node &node : coord) {
      coord_ips.push_back(node.as<Address>());
    }

    if (coord_ips.size() <= 0) {
      std::cerr << "No coordinator found" << std::endl;
      exit(1);
    }

    vector<HandlerThread> threads;
    for (Address addr : coord_ips) {
      for (unsigned i = 0; i < coordThreadCount; i++) {
        threads.push_back(HandlerThread(addr, i));
      }
    }

    vector<Address> kvs_routing_ips;
    if (YAML::Node elb = user["routing-elb"]) {
      kvs_routing_ips.push_back(elb.as<Address>());
    }

    vector<UserRoutingThread> kvs_routing_threads;
    for (Address addr : kvs_routing_ips) {
      for (unsigned i = 0; i < 1; i++) {
        kvs_routing_threads.push_back(UserRoutingThread(addr, i));
      }
    }

    kvs_clients.push_back(new KvsClient(kvs_routing_threads, ip, thread_id + 1, 30000));
    CommHelper helper(threads, ip, thread_id, 30000);
    string log_file = "log_executor_" + std::to_string(thread_id) + ".txt";
    string log_name = "log_executor_" + std::to_string(thread_id);
    auto log = spdlog::basic_logger_mt(log_name, log_file, true);
    log->flush_on(spdlog::level::info);
    
    for (auto kvs_client : kvs_clients){
      kvs_client->set_logger(log);
    }

    helper.set_logger(log);

    run(&helper, ip, thread_id, log);
}


