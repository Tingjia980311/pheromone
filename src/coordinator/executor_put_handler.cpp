#include "coord_handlers.hpp"
#include <sstream>
#include <iterator>

extern unsigned seed;
extern unsigned io_thread_num;
// extern unsigned io_thread_num;

// const int batchObjectThreshold = 10;
// const char* batchDelim = "|";

// std::hash<string> hasher;

uint32_t schedule_func_call_(map<uint32_t, uint32_t> &executor_status_map){
    vector<uint32_t> avail_executors;
    for (auto &executor_status : executor_status_map) {
    if (executor_status.second == 1 || executor_status.second == 0){
        // find an available warm node, just move forward
        avail_executors.push_back(executor_status.first);
    }
    }

    if (avail_executors.size() > 0){
    auto executor_id = 0;
    executor_id = avail_executors[rand_r(&seed) % avail_executors.size()];
    return executor_id;
    }
    return -1;
}

void invoke_tgtfunc_call(logger log, 
                string &app_name, string &resp_address, vector<string> &tgt_functions,
                vector<vector<string>> &func_arg_vector,
                vector<vector<int>> &func_arg_flags,
                SocketCache &pushers,
                map<Address, NodeStatus> &node_status_map, 
                map<Address, map<uint32_t, uint32_t>> &executor_status_map,
                map<string, string> &key_val_map){
    auto receive_req_stamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    FunctionCall call_msg;
    call_msg.set_app_name(app_name);
    call_msg.set_resp_address(resp_address);
    for (int i = 0; i < tgt_functions.size(); i++) {
        auto req = call_msg.add_requests();
        req->set_name(tgt_functions[i]);
        for (int j = 0; j < func_arg_vector[i].size(); j++) {
        auto arg = req->add_arguments();
        arg->set_body(key_val_map[func_arg_vector[i][j]]);
        arg->set_arg_flag(func_arg_flags[i][j]);
        }
    }
    string serialized;
    call_msg.SerializeToString(&serialized);

    map<Address, string> scheduled_node_msg;
    auto req_num = tgt_functions.size();

    int max_executors = 0;
    Address node_with_max_executors;

    if (req_num > 1) {
        int total_executors = 0;
        vector<pair<Address, int>> avail_nodes;
        for (auto &pair : node_status_map){
            if (pair.second.avail_executors_ > 0){
            total_executors += pair.second.avail_executors_;
            avail_nodes.push_back(std::make_pair(pair.first, pair.second.avail_executors_));
            }
            if(pair.second.avail_executors_ > max_executors){
            max_executors = pair.second.avail_executors_;
            node_with_max_executors = pair.first;
            }
        }

        if (max_executors >= req_num) {
            scheduled_node_msg[node_with_max_executors] = serialized;
        }
        else {
            if (total_executors >= req_num) {
                vector<int> node_loads(avail_nodes.size());

                int node_idx = 0;
                for(int i = 0; i < req_num; i++){
                    while (avail_nodes[node_idx].second <= 0){
                    node_idx = (node_idx + 1) % avail_nodes.size();
                    }
                    node_loads[node_idx]++;
                    avail_nodes[node_idx].second--;
                    node_idx = (node_idx + 1) % avail_nodes.size();
                }

                FunctionCall routed_call;
                routed_call.set_app_name(app_name);
                routed_call.set_resp_address(resp_address);
                // routed_call.set_response_key(response_key);
                int avail_node_index = 0;
                int cur_load = 0;
                for (auto &req : call_msg.requests()){
                    auto func_req = routed_call.add_requests();
                    func_req->CopyFrom(req);
                    cur_load++;
                    if (cur_load == node_loads[avail_node_index]){
                    string part_serialized;
                    routed_call.SerializeToString(&part_serialized);
                    scheduled_node_msg[avail_nodes[avail_node_index].first] = part_serialized;
                    avail_node_index++;
                    cur_load = 0;
                    routed_call.clear_requests();
                    }
                }

            }
        }
    }
    else {
        for (auto &pair : node_status_map){
            if(pair.second.avail_executors_ > max_executors){
            max_executors = pair.second.avail_executors_;
            node_with_max_executors = pair.first;
            }
        }
        if (max_executors > 0) {
            scheduled_node_msg[node_with_max_executors] = serialized;
        }
        else {
            // work around for fully utilizing executors
            if (node_status_map.size() > 0) {
            auto it = node_status_map.begin();
            std::advance(it, rand_r(&seed) % node_status_map.size());
            scheduled_node_msg[it->first] = serialized;
            }
        }
    }

    if (scheduled_node_msg.empty()){
        auto return_stamp = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        log->info("No worker for app function call {}. req: {}, recv: {}, return: {}", app_name, req_num, receive_req_stamp, return_stamp);
        if (!resp_address.empty()){
        FunctionCallResponse resp;
        resp.set_app_name(app_name);
        resp.set_request_id(call_msg.request_id());
        resp.set_error_no(1);

        string resp_serialized;
        resp.SerializeToString(&resp_serialized);
        kZmqUtil->send_string(resp_serialized, &pushers[resp_address]);
        }
    }
    else{
        auto scheduled_stamp = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        for (auto &node_msg : scheduled_node_msg){
        string func_exec_addr = get_func_exec_address(node_msg.first, rand_r(&seed) % io_thread_num);
        uint32_t executor_id = schedule_func_call_(executor_status_map[node_msg.first] );
        string executor_func_call_addr = get_executor_func_call_address(node_msg.first, executor_id);
        // kZmqUtil->send_string(node_msg.second, &pushers[func_exec_addr]);
        if (executor_id == -1) {
            FunctionCallResponse resp;
            resp.set_app_name(app_name);
            resp.set_request_id(call_msg.request_id());
            resp.set_error_no(1);

            string resp_serialized;
            resp.SerializeToString(&resp_serialized);
            kZmqUtil->send_string(resp_serialized, &pushers[resp_address]);
        }
        kZmqUtil->send_string(node_msg.second, &pushers[executor_func_call_addr]);
        executor_status_map[node_msg.first][executor_id] = 2;
        }
    }
}

void executor_put_handler(logger log, string &serialized, SocketCache &pushers,
                    map<Address, NodeStatus> &node_status_map, 
                    map<Address, map<uint32_t, uint32_t>> &executor_status_map,
                    map<string, AppInfo> &app_info_map,
                    map<string, string> &key_val_map,
                    map<string, unsigned> &key_len_map,
                    map<string, string> &func_app_map) {
    ExecutorPutMessage msg;
    msg.ParseFromString(serialized);
    log->info ("src func: {}, tgt func: {}", msg.func_name(), msg.tgt_function());
    auto req_id = msg.request_id();
    auto resp_address = msg.resp_address();
    auto if_anna = msg.if_anna();
    auto src_function = msg.func_name();
    auto tgt_function = msg.tgt_function();
    auto object_name = msg.object_name();
    vector<string> infos;
    split(object_name, '|', infos);
    auto bucket = infos[0];
    auto key = infos[1];
    auto option = msg.option();
    string app_name = func_app_map[src_function];


    if (option == 2) {

        if (!if_anna) {
            key_val_map[object_name] = msg.data();
            key_len_map[object_name] = key_val_map[object_name].size();
        } else {
            key_val_map[object_name] = msg.object_name();
            key_len_map[object_name] = key_val_map[object_name].size();
        }

        if (app_info_map[app_name].direct_deps_.find(src_function) != app_info_map[app_name].direct_deps_.end()){
            vector<string> func_args;
            func_args.push_back(key_val_map[object_name]);
            if (tgt_function.empty()){
                vector<string> target_funcs;
                vector<vector<string>> func_arg_vector;
                vector<vector<int>> func_arg_flags;
                for (auto target_dep : app_info_map[app_name].direct_deps_[src_function]) {
                    target_funcs.push_back(target_dep);
                    vector<string> args;
                    args.push_back(object_name);
                    func_arg_vector.push_back(args);
                    vector<int> flags;
                    flags.push_back(if_anna);
                    func_arg_flags.push_back(flags);
                }
                invoke_tgtfunc_call(log, app_name, resp_address, target_funcs,
                                    func_arg_vector,
                                    func_arg_flags,
                                    pushers,
                                    node_status_map, 
                                    executor_status_map,
                                    key_val_map);

            }          
        }

        
    } else {
        // string output_data;
        // output_data = key_val_map[object_name];

        // helper->client_response(resp_address, func_app_map[src_function], output_data);
        key_val_map[object_name] = msg.data();
        key_len_map[object_name] = key_val_map[object_name].size();
        FunctionCallResponse client_resp;
        client_resp.set_app_name(app_name);
        client_resp.set_error_no(0);
        client_resp.set_output(key_val_map[object_name]);
        string resp_serialized;
        client_resp.SerializeToString(&resp_serialized);
        kZmqUtil->send_string(resp_serialized, &pushers[resp_address]);
        
    }


  
}
