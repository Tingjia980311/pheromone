
while IFS='' read -r line || [[ -n "$line" ]] ; do
  kill -9 $line
done < "pids"

if [ "$1" = "y" ]; then
  rm *log*
fi

rm pids