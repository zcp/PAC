
#!/bin/bash
#usage: ./collect.sh cpu_file disk_file net_file

echo $1

sar_filename=$1-"`hostname`"

#lauch sar
nice -n -1 sar -u -r -d  -n DEV 1 > ${sar_filename} &

#collectl_pid=$!



