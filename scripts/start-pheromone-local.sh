

./build/target/manager local &
MPID=$!
./build/target/coordinator local &
CPID=$!
# ./build/target/scheduler local &
# SPID=$!
./build/target/executor 0 &
E1PID=$!
./build/target/executor 1 &
E2PID=$!


# echo $SPID > pids
echo $E1PID >> pids
echo $E2PID >> pids
echo $MPID >> pids
echo $CPID >> pids