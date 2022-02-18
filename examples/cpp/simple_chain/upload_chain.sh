set -x

func_nodes=$(kubectl get pod | grep func | cut -d " " -f 1 | tr -d " ")
for pod in ${func_nodes[@]}; do
    kubectl cp read_func.so $pod:/dev/shm -c local-sched &
done

wait

set -x

func_nodes=$(kubectl get pod | grep func | cut -d " " -f 1 | tr -d " ")
for pod in ${func_nodes[@]}; do
    kubectl cp write_func.so $pod:/dev/shm -c local-sched &
done

func_nodes=$(kubectl get pod | grep func | cut -d " " -f 1 | tr -d " ")
for pod in ${func_nodes[@]}; do
    kubectl cp /home/ubuntu/pheromone/include/scheduler/comm_helper.hpp $pod:/pheromone/include/scheduler/ -c local-sched &
done


wait