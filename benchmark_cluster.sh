#!/bin/bash


master_ip=192.168.10.4
slave1_ip=192.168.10.11
slave2_ip=192.168.10.12
slave3_ip=192.168.10.13
slave4_ip=192.168.10.14
slave5_ip=192.168.10.15
slave6_ip=192.168.10.16

master_hostname=worker-2
result_hostname=worker-2
#result_hostname=localhost
slave1_hostname=worker-1
slave2_hostname=worker-2
slave3_hostname=worker-3
slave4_hostname=worker-4
slave5_hostname=worker-5
slave6_hostname=worker-6

store1_hostname=store-1
store2_hostname=store-2

####--------------very important notation--------------------
#Exception in thread "main" org.apache.spark.SparkException: Cluster deploy mode is currently not 
#supported for python applications on standalone clusters.
#we luach the benchmark on k8s-master and
#spark master will actually run slave2_hostname
 
#node0 is the master
nodes=($slave2_hostname $slave1_hostname $slave4_hostname $slave6_hostname)
hdfs_nodes=($store1_hostname $store2_hostname)

#nodes=(localhost)
node_num=${#nodes[@]}
range=`expr $node_num - 1`

#spark_home=/opt/spark_compression
spark_home=/opt/spark_shuffle_files

benchmark_dir=$SPARK_COMPRESSION_HOME
execution_dir=$spark_home
monitor_dir=/tmp/monitor
#spark_home=/opt/spark
#spark_home=/opt/my_spark
monitor_script=collectl.sh
kill_script=kill_bash.sh

collectl_cpu=$monitor_dir/cpu.txt
collectl_disk=$monitor_dir/disk.txt
collectl_mem=$monitor_dir/mem.txt
collectl_net=$monitor_dir/net.txt
sar_file=$monitor_dir/sar

sync_time(){
   for i in $( eval echo {0..$range} ); do        
       node=${nodes[$i]}
       echo "sync time of $node"
       sudo sshpass -p 123456 ssh $node "ntpdate -u $master_hostname"
   done

   #for hdfs cluster
   for node in $store1_hostname $store2_hostname; do
       echo "sync time of $node"
       sudo sshpass -p 123456 ssh $node "ntpdate -u $master_hostname"
   done
}

mask_ssh_confirm(){
   #for master
   sudo bash -c 'echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config'
   #for other hosts.
   for i in $( eval echo {1..$range} ); do
       node=${nodes[$i]}
       sudo ssh $node "bash -c 'echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config'"
   done
}


clear_cache(){
   for i in $( eval echo {0..$range} ); do

       node=${nodes[$i]}
       sudo sshpass -p 123456 ssh -t $node "sync; sudo sysctl vm.drop_caches=1; echo 1 > /proc/sys/vm/drop_caches; echo 2 > /proc/sys/vm/drop_caches; echo 3 > /proc/sys/vm/drop_caches" &

    done
}

start_monitor(){
    echo "start monitor"

    for i in $( eval echo {0..$range} ); do
       node=${nodes[$i]}
       sudo sshpass -p 123456 ssh $node "$monitor_dir/$monitor_script $sar_file" & 
    done
    #for hdfs cluster
    for node in $store1_hostname $store2_hostname; do
       sudo sshpass -p 123456 ssh $node "$monitor_dir/$monitor_script $sar_file" & 
    done
}

clean_monitor(){
   for i in $( eval echo {0..$range} ); do
       node=${nodes[$i]}
       echo "clear monitor on" $node  
       #sudo sshpass -p 123456 ssh -t $node "sudo $monitor_dir/$kill_script collectl"
       sudo sshpass -p 123456 ssh -t $node "sudo $monitor_dir/$kill_script sar"
   done
   #for hdfs cluster
   for node in $store1_hostname $store2_hostname; do
   sudo sshpass -p 123456 ssh -t $node "sudo $monitor_dir/$kill_script sar"
   done
}


copyFiles(){
   #echo "copy files on master"
   #mkdir -p $monitor_dir
   #cd  $benchmark_dir; 
   #cp  $monitor_script $kill_script $monitor_dir

   #sudo ssh $master_hostname  "mkdir -p $monitor_dir"
   mkdir -p /tmp/monitor/lda_temp_files
   
   for i in $( eval echo {0..$range} ); do
       node=${nodes[$i]}
       echo "make monitor dir on" $node 
       sudo sshpass -p 123456 ssh $node "mkdir -p $monitor_dir;"
       cd  $benchmark_dir;                              
       sudo sshpass -p 123456 scp $monitor_script $kill_script $node:$monitor_dir 
   done
   
   #for hdfs cluster
   for node in $store1_hostname $store2_hostname; do
       echo "make monitor dir on" $node 
       sudo sshpass -p 123456 ssh $node "mkdir -p $monitor_dir;"
       cd  $benchmark_dir;                              
       sudo sshpass -p 123456 scp $monitor_script $kill_script $node:$monitor_dir      
   done
}

delFiles(){
  #echo "delete files on master"
  #delete files on master
  #sudo rm -r $monitor_dir;

  for i in $( eval echo {0..$range} ); do
     node=${nodes[$i]}
     echo "delete files" on $node
     sudo sshpass -p 123456 ssh -t $node  "sudo rm -r $monitor_dir; sudo rm -r /tmp/spark-events/*; sudo rm -r /tmp/spark.log;";
     sudo sshpass -p 123456 ssh -t $node  "sudo rm -r $spark_home/work/*";
  done

  #for hdfs cluster
  for node in $store1_hostname $store2_hostname; do
     sudo sshpass -p 123456 ssh -t $node  "sudo rm -r $monitor_dir;"
  done
}


run_terasort(){
   cd $micro_dir
   driver_cores=4
   driver_memory=22000M
   executor_cores=2
   executor_memory=8000M
   total_cores=`expr $executor_num \* $executor_cores`

  
   result_loc=$monitor_dir/terasort_result
   #data=$hdfs_dir/run_sort/10240
   #We submit the program on master node so that the driver will run on the node.
   echo "run terasort from HiBench micro"

    $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 \
                            --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
                            --driver-cores $driver_cores \
                            --driver-memory $driver_memory \
                            --executor-cores $executor_cores \
                            --executor-memory $executor_memory \
                            --total-executor-cores $total_cores \
                            --class com.github.ehiggs.spark.terasort.TeraSort /home/paper_4_fgcs/spark-terasort-master_nowriteback/target/spark-terasort-1.2-SNAPSHOT-jar-with-dependencies.jar $compression_algorithm $compression_buffersize $compression_flag  $compression_write_opti $compression_read_opti $performance_trace \
                            $mylogger_flag $raw_sample $shuffle_file_save hdfs://store-1:9000/HiBench/Terasort_in hdfs://store-1:9000/HiBench/Terasort_out/ > $result_loc 2>&1
}


run_sort(){
   cd $micro_dir
   driver_cores=4
   driver_memory=22000M
   executor_cores=2
   executor_memory=8000M
   total_cores=`expr $executor_num \* $executor_cores`

  
   result_loc=$monitor_dir/sort_result
   #data=$hdfs_dir/run_sort/10240
   #We submit the program on master node so that the driver will run on the node.
   echo "run sort from HiBench micro"

   $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 \
                            --conf "spark.network.timeout=1000000" \
                            --conf "spark.executor.heartbeatInterval=1000000" \
                            --conf "spark.driver.maxResultSize=10000M" \
                            --driver-cores $driver_cores \
                            --driver-memory $driver_memory \
                            --executor-cores $executor_cores \
                            --executor-memory $executor_memory \
                            --total-executor-cores $total_cores \
                            --class org.apache.spark.examples.SortTest  $spark_home/examples/jars/spark-examples_2.12-3.0.3.jar \
                            $compression_algorithm $compression_buffersize $compression_flag  $compression_write_opti $compression_read_opti $performance_trace \
                            $mylogger_flag $raw_sample $shuffle_file_save hdfs://store-1:9000/HiBench/Wordcount/Input > $result_loc 2>&1
}

run_wordcount(){
   cd $micro_dir
   driver_cores=1
   driver_memory=2048M
   executor_cores=1
   executor_memory=2048M
   total_cores=`expr $executor_num \* $executor_cores`

  
   result_loc=$monitor_dir/wordcount_result
   #data=$hdfs_dir/run_sort/10240
   #We submit the program on master node so that the driver will run on the node.
   echo "run wordcount from HiBench micro"

    $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 \
                            --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
                            --driver-cores $driver_cores \
                            --driver-memory $driver_memory \
                            --executor-cores $executor_cores \
                            --executor-memory $executor_memory \
                            --total-executor-cores $total_cores \
                            --class org.apache.spark.examples.WordcountTest  $spark_home/examples/jars/spark-examples_2.12-3.0.3.jar \
			    $compression_algorithm $compression_buffersize $compression_flag  $compression_write_opti $compression_read_opti $performance_trace \
                            $mylogger_flag $raw_sample $shuffle_file_save hdfs://store-1:9000/HiBench/Wordcount/Input > $result_loc 2>&1
}

run_kmeans(){
   cd $micro_dir
   driver_cores=1
   driver_memory=2048M
   executor_cores=1
   executor_memory=2048M
   total_cores=`expr $executor_num \* $executor_cores`

  
   result_loc=$monitor_dir/kmeans_result
   #data=$hdfs_dir/run_sort/10240
   #We submit the program on master node so that the driver will run on the node.
   echo "run kmeans from HiBench micro"

    $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 \
                            --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
                            --driver-cores $driver_cores \
                            --driver-memory $driver_memory \
                            --executor-cores $executor_cores \
                            --executor-memory $executor_memory \
                            --total-executor-cores $total_cores \
                            --class org.apache.spark.examples.KMeansTest  $spark_home/examples/jars/spark-examples_2.12-3.0.3.jar \
			    $compression_algorithm $compression_buffersize $compression_flag  $compression_write_opti $compression_read_opti $performance_trace \
                            $mylogger_flag $raw_sample $shuffle_file_save hdfs://store-1:9000/5G 1 > $result_loc 2>&1
}

run_lda(){
   #generate dataset for lda
   #cd /opt/HiBench_compression/HiBench-master/bin/workloads/ml/lda/prepare
   #./my_prepare.sh $data_size
   #sleep 1m
   cd $micro_dir
   driver_cores=1
   driver_memory=2048M
   executor_cores=1
   executor_memory=2048M
   total_cores=`expr $executor_num \* $executor_cores`

  
   result_loc=$monitor_dir/lda_result
   #data=$hdfs_dir/run_sort/10240
   #We submit the program on master node so that the driver will run on the node.
   echo "run LDA from HiBench micro"

   rm -r /tmp/lda_output
   
   $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 \
                            --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
                            --driver-cores $driver_cores \
                            --driver-memory $driver_memory \
                            --executor-cores $executor_cores \
                            --executor-memory $executor_memory \
                            --total-executor-cores $total_cores \
                            --class org.apache.spark.examples.LDATest  $spark_home/examples/jars/spark-examples_2.12-3.0.3.jar \
			    $compression_algorithm $compression_buffersize $compression_flag  $compression_write_opti $compression_read_opti $performance_trace \
                            $mylogger_flag $raw_sample $shuffle_file_save hdfs://store-1:9000/HiBench/LDA/Input /tmp/lda_output 2 > $result_loc 2>&1
}

run_linear(){
   cd $micro_dir
   driver_cores=1
   driver_memory=2048M
   executor_cores=1
   executor_memory=2048M

   total_cores=`expr $executor_num \* $executor_cores`

  
   result_loc=$monitor_dir/linear_result
   #data=$hdfs_dir/run_sort/10240
   #We submit the program on master node so that the driver will run on the node.
   echo "run linear regression from HiBench micro"

   $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 \
                            --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
                            --driver-cores $driver_cores \
                            --driver-memory $driver_memory \
                            --executor-cores $executor_cores \
                            --executor-memory $executor_memory \
                            --total-executor-cores $total_cores \
                            --class org.apache.spark.examples.LinearRegressionWithElasticNet  $spark_home/examples/jars/spark-examples_2.12-3.0.3.jar \
                            $compression_algorithm $compression_buffersize $compression_flag  $compression_write_opti $compression_read_opti $performance_trace \
                            $mylogger_flag $raw_sample $shuffle_file_save hdfs://store-1:9000/HiBench/Linear/Input 2 > $result_loc 2>&1
}

run_logistic(){
   cd $micro_dir
   driver_cores=1
   driver_memory=4048M
   executor_cores=1
   executor_memory=2048M

   total_cores=`expr $executor_num \* $executor_cores`

  
   result_loc=$monitor_dir/logistic_result
   #data=$hdfs_dir/run_sort/10240
   #We submit the program on master node so that the driver will run on the node.
   echo "run logistic regression from HiBench micro"

   $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 \
                            --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
                            --conf "spark.network.timeout=1000000" \
                            --conf "spark.executor.heartbeatInterval=1000000" \
                            --conf "spark.driver.maxResultSize=2048M" \
                            --driver-cores $driver_cores \
                            --driver-memory $driver_memory \
                            --executor-cores $executor_cores \
                            --executor-memory $executor_memory \
                            --total-executor-cores $total_cores \
                            --class org.apache.spark.examples.LogisticRegression  $spark_home/examples/jars/spark-examples_2.12-3.0.3.jar \
                            $compression_algorithm $compression_buffersize $compression_flag  $compression_write_opti $compression_read_opti $performance_trace \
                            $mylogger_flag $raw_sample $shuffle_file_save hdfs://store-1:9000/HiBench/LR/Input > $result_loc 2>&1
}

run_pagerank(){
   cd $micro_dir
   driver_cores=1
   driver_memory=2048M
   executor_cores=1
   executor_memory=2048M

   total_cores=`expr $executor_num \* $executor_cores`

  
   result_loc=$monitor_dir/pagerank_result
   #data=$hdfs_dir/run_sort/10240
   #We submit the program on master node so that the driver will run on the node.
   echo "run pagerank from HiBench micro"

   hdfs dfs -rm -r hdfs://store-1:9000/HiBench/Pagerank/Output
   $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 \
                            --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
                            --driver-cores $driver_cores \
                            --driver-memory $driver_memory \
                            --executor-cores $executor_cores \
                            --executor-memory $executor_memory \
                            --total-executor-cores $total_cores \
                            --class org.apache.spark.examples.PageRankTest  $spark_home/examples/jars/spark-examples_2.12-3.0.3.jar \
                            $compression_algorithm $compression_buffersize $compression_flag  $compression_write_opti $compression_read_opti $performance_trace \
                            $mylogger_flag $raw_sample $shuffle_file_save hdfs://store-1:9000/HiBench/Pagerank/Input/edges \
                            hdfs://store-1:9000/HiBench/Pagerank/Output 2 > $result_loc 2>&1
}

run_rf(){
   #cd /opt/HiBench_compression/HiBench-master/bin/workloads/ml/rf/prepare
   #./my_prepare.sh $data_size
   #sleep 1m
   cd $micro_dir
   driver_cores=1
   driver_memory=2048M
   executor_cores=1
   executor_memory=2048M

   total_cores=`expr $executor_num \* $executor_cores`

  
   result_loc=$monitor_dir/rf_result
   #data=$hdfs_dir/run_sort/10240
   #We submit the program on master node so that the driver will run on the node.
   echo "run random forest from HiBench micro"

   $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 \
                            --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
                            --driver-cores $driver_cores \
                            --driver-memory $driver_memory \
                            --executor-cores $executor_cores \
                            --executor-memory $executor_memory \
                            --total-executor-cores $total_cores \
                            --class org.apache.spark.examples.RandomForestTest  $spark_home/examples/jars/spark-examples_2.12-3.0.3.jar \
                            $compression_algorithm $compression_buffersize $compression_flag  $compression_write_opti $compression_read_opti $performance_trace \
                            $mylogger_flag $raw_sample $shuffle_file_save hdfs://store-1:9000/HiBench/RF/Input > $result_loc 2>&1
}

run_svm(){
   cd $micro_dir
   driver_cores=1
   driver_memory=2048M
   executor_cores=1
   executor_memory=2048M

   total_cores=`expr $executor_num \* $executor_cores`

  
   result_loc=$monitor_dir/svm_result
   #data=$hdfs_dir/run_sort/10240
   #We submit the program on master node so that the driver will run on the node.
   echo "run svm from HiBench micro"

   $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 \
                            --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
                            --driver-cores $driver_cores \
                            --driver-memory $driver_memory \
                            --executor-cores $executor_cores \
                            --executor-memory $executor_memory \
                            --total-executor-cores $total_cores \
                            --class org.apache.spark.examples.SVMWithSGDExample  $spark_home/examples/jars/spark-examples_2.12-3.0.3.jar \
                            $compression_algorithm $compression_buffersize $compression_flag  $compression_write_opti $compression_read_opti $performance_trace \
                            $mylogger_flag $raw_sample $shuffle_file_save hdfs://store-1:9000/HiBench/SVM/Input 2 > $result_loc 2>&1
}

run_sqljoin(){
   cd $micro_dir
   driver_cores=1
   driver_memory=2048M
   executor_cores=1
   executor_memory=2048M

   total_cores=`expr $executor_num \* $executor_cores`

  
   result_loc=$monitor_dir/sqljoin_result
   #data=$hdfs_dir/run_sort/10240
   #We submit the program on master node so that the driver will run on the node.
   echo "run sql join from HiBench micro"

   $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 \
                            --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
                            --driver-cores $driver_cores \
                            --driver-memory $driver_memory \
                            --executor-cores $executor_cores \
                            --executor-memory $executor_memory \
                            --total-executor-cores $total_cores \
                            --class org.apache.spark.examples.SQLJoinTest  $spark_home/examples/jars/spark-examples_2.12-3.0.3.jar \
                            $compression_algorithm $compression_buffersize $compression_flag  $compression_write_opti $compression_read_opti $performance_trace \
                            $mylogger_flag $raw_sample $shuffle_file_save hdfs://localhost:9000/large_big_table \
                            hdfs://store-1:9000/big_table> $result_loc 2>&1
}

genData_terasort(){
   $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 --class com.github.ehiggs.spark.terasort.TeraGen /home/paper_4_fgcs/spark-terasort-master/target/spark-terasort-1.2-SNAPSHOT-jar-with-dependencies.jar $terasort_datasize hdfs://HiBench/Terasort
}


#sort and wordcount use the same dataset
genData_wordcount(){
  cd $micro_dir
  ./wordcount/prepare/my_prepare.sh $data_size
}

rmData_wordcount(){
  cd $micro_dir
  ./wordcount/prepare/rm_data.sh

}

genData_pagerank(){
   cd $pr_benchmark_dir
   echo $pr_benchmark_dir
  ./prepare/my_prepare.sh $data_size
}

rmData_pagerank(){
   cd $pr_benchmark_dir
   echo $pr_benchmark_dir
  ./prepare/rm_data.sh

}


#all sql benchmarks use the same dataset
genData_sql(){  
  cd $benchmark_dir/HiBench-master/bin/workloads/sql
  ./scan/prepare/my_prepare.sh $data_size
}

rmData_sql(){
  cd $benchmark_dir/HiBench-master/bin/workloads/sql
  ./scan/prepare/rm_data.sh

}


genData_ml(){
   cd $ml_benchmark_dir
   echo $ml_benchmark_dir
  ./prepare/my_prepare.sh $data_size

}

rmData_ml(){
   cd $ml_benchmark_dir
   echo $ml_benchmark_dir

  ./prepare/rm_data.sh

}



start_master(){
   #node0 is the master
   node=${nodes[0]}
   echo "start master" on $node
   sshpass -p 123456 ssh $node "cd $execution_dir/sbin; ./start-master.sh"
   #close spark history server
   sshpass -p 123456 ssh $node "mkdir -p /tmp/spark-events; cd $execution_dir/sbin; ./start-history-server.sh"
}

close_master(){
   #node0 is the master
   node=${nodes[0]}
   echo "close master" on $node
   echo $execution_dir
   sshpass -p 123456 ssh $node "cd $execution_dir/sbin; ./stop-master.sh"
   sshpass -p 123456 ssh $node "cd $execution_dir/sbin; ./stop-history-server.sh"
}

start_slaves(){
   #master is also a slave in the cluster
   for i in $( eval echo {0..$range} ); do
       node=${nodes[$i]}
       echo "start slave" on $node
       echo $execution_dir
       sshpass -p 123456 ssh $node "mkdir -p /tmp/spark-events; cd $execution_dir/sbin; ./start-slave.sh spark://${nodes[0]}:7077"
   done
}

close_slaves(){
   for i in $( eval echo {0..$range} ); do
       node=${nodes[$i]}
       echo $execution_dir
       echo "close slave" on $node
       sshpass -p 123456 ssh $node "cd $execution_dir/sbin; ./stop-slave.sh;"
   done
}

transFiles(){
   echo "transfer Files back"
   result_hostname=$result_hostname
   dest_dir=/home/results/$storage;
   #sudo ssh -p $result_port $result_host "mkdir -p $dest_dir"
   mkdir -p $dest_dir
   cp /etc/hosts $dest_dir
   #cp  $monitor_dir/* $dest_dir
   for i in $( eval echo {0..$range} ); do
       node=${nodes[$i]}
       echo "transfer files back" on $node
       sudo sshpass -p 123456 ssh $node "sshpass -p 123456 scp -r $monitor_dir/* $result_hostname:$dest_dir";
       sudo sshpass -p 123456 ssh $node "sshpass -p 123456 scp -r /tmp/spark.log $result_hostname:$dest_dir/spark.log-$node";
       #copy spark history 
       sudo sshpass -p 123456 ssh $node "sshpass -p 123456 scp -r /tmp/spark-events/* $result_hostname:$dest_dir";
       sudo sshpass -p 123456 ssh $node "mkdir -p $result_hostname:$dest_dir/gclog-$node; sshpass -p 123456 scp -r $spark_home/work  $result_hostname:$dest_dir/gclog-$node";
       sudo sshpass -p 123456 ssh $node "sshpass -p 123456 scp -r /tmp/libz_details-* $result_hostname:$dest_dir";
   done

   #for hdfs cluster
   for node in $store1_hostname $store2_hostname; do  
      sudo sshpass -p 123456 ssh $node "sshpass -p 123456 scp -r $monitor_dir/* $result_hostname:$dest_dir";
   done

}

zlib_switch(){
   if [[ $zlib_version  == "cloudflare" ]]; then 
     zlib_dir=/opt/zlib-cloudflare
     zlib_file=/opt/zlib-cloudflare.tar
   fi
   if [[ $zlib_version  == "intel" ]]; then 
     zlib_dir=/opt/zlib-new
     zlib_file=/opt/zlib-new.tar
   fi
   if [[ $zlib_version  == "native" ]]; then 
     zlib_dir=/opt/zlib-native
     zlib_file=/opt/zlib-native.tar
   fi 
   
   for i in $( eval echo {0..$range} ); do
       node=${nodes[$i]}
       echo "update zlib " on $node
       echo $zlib_dir
       echo $zlib_file
       sshpass -p 123456 ssh $node "rm -r $zlib_dir; cd /opt; tar -xzvf $zlib_file; cd $zlib_dir; ./configure; make; make install; ls -alt /usr/local/lib > /tmp/libz_details-$node 2>&1"
   done
}


generate_data(){
          if [[ "$run_benchmark" == "run_terasort" ]]; then
              if [[ "$data_size" == "small" ]]; then
                   terasort_datasize=320m
              fi
              if [[ "$data_size" == "large" ]]; then
                   terasort_datasize=3200m
              fi
              if [[ "$data_size" == "huge" ]]; then
                   terasort_datasize=32g
              fi
              if [[ "$data_size" == "gigantic" ]]; then
                   terasort_datasize=320g
              fi
              if [[ "$data_size" == "bigdata" ]]; then
                   terasort_datasize=500g
              fi

              $spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 --class com.github.ehiggs.spark.terasort.TeraGen /home/paper_4_fgcs/spark-terasort-master/target/spark-terasort-1.2-SNAPSHOT-jar-with-dependencies.jar $terasort_datasize hdfs://store-1:9000/HiBench/Terasort_in
            sleep 10m
          fi

          if [[ "$run_benchmark" == "run_pagerank" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/websearch/pagerank/prepare
            ./my_prepare.sh $data_size
            sleep 3m
          fi

          if [[ "$run_benchmark" == "run_wordcount" ]]; then
             cd /opt/HiBench_compression/HiBench-master/bin/workloads/micro/wordcount/prepare
             ./my_prepare.sh $data_size
             sleep 3m
          fi
          if [[ "$run_benchmark" == "run_sort" ]]; then
             cd /opt/HiBench_compression/HiBench-master/bin/workloads/micro/sort/prepare
             ./my_prepare.sh $data_size
             sleep 3m
          fi
          if [[ "$run_benchmark" == "run_lda" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/ml/lda/prepare
            ./my_prepare.sh $data_size
            sleep 3m
          fi
          if [[ "$run_benchmark" == "run_rf" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/ml/rf/prepare
            ./my_prepare.sh $data_size
            sleep 3m
          fi
          if [[ "$run_benchmark" == "run_logistic" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/ml/lr/prepare
            ./my_prepare.sh $data_size
            sleep 3m
          fi
          if [[ "$run_benchmark" == "run_linear" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/ml/linear/prepare
            ./my_prepare.sh $data_size
            sleep 3m
          fi
          if [[ "$run_benchmark" == "run_svm" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/ml/svm/prepare
            ./my_prepare.sh $data_size
            sleep 3m
          fi
}

test_zlibs(){
      for zlib_version in native intel cloudflare; do
          file_format=$zlib_version
          zlib_switch
          for run_benchmark in run_sort; do
          #data have generated in test_others
              compression_algorithm=kaezip
              for executor_num in 20; do
                 if [[ "$run_benchmark" == "run_sort" ]]; then
                     executor_num=4
                 fi
                for count in 1 2 3 ; do
                  delFiles
                  clear_cache
                  copyFiles
                  clean_monitor
                  start_monitor
                  $run_benchmark
                  clean_monitor
                  storage=E$executor_num-$count-$data_size-$compression_algorithm-$file_format-$run_benchmark$zlib_version
                  echo $storage
                  
                  transFiles
                  #delFiles
                done
             done
       done
     done
}

test_others(){ 
#run_lda run_rf run_pagerank run_linear run_logistic  run_svm
       for run_benchmark in run_terasort ; do
          generate_data
          for compression_algorithm in lz4 lzf snappy zstd kaezip; do
           for executor_num in 20; do
                 if [[ "$run_benchmark" == "run_sort" ]]; then
                     executor_num=4
                 fi
                 if [[ "$run_benchmark" == "run_terasort" ]]; then
                     executor_num=4
                 fi
             for count in 1 2 3; do
                  delFiles
                  clear_cache
                  copyFiles
                  clean_monitor
                  start_monitor
                  $run_benchmark
                  clean_monitor
                  storage=E$executor_num-$count-$data_size-$compression_algorithm-$file_format-$run_benchmark$zlib_version
                  echo $storage
                  transFiles
                  #delFiles
             done
         done
         done
      done
}

test_zlib_others(){
   compression_algorithm=kaezip
   compression_buffersize=32
   compression_flag=true
   compression_write_opti=t
   compression_read_opti=t
   performance_trace=f
   mylogger_flag=f
   raw_sample=f
   shuffle_file_save=f

   zlib_version=native
   file_format=$zlib_version
   zlib_switch
   for data_size in bigdata large; do
      #zlib_version=native
      #file_format=$zlib_version
      #zlib_switch
      test_others
      #sleep 10m
      #test_zlibs
   done
}




test_compression_tracer(){
   compression_algorithm=kaezip
   compression_buffersize=32
   compression_flag=true
   compression_write_opti=t
   compression_read_opti=t
   performance_trace=f
   mylogger_flag=f
   raw_sample=f
   shuffle_file_save=t

   for data_size in huge; do
       file_format=text
       #zlib_version=native
       #zlib_switch
       for zlib_version in native ; do
          zlib_switch
          file_format=$zlib_version
          ls -alt /usr/local/lib > $monitor_dir/libz_details
       for run_benchmark in run_rf ; do
          sudo rm -r /home/compression_tracer
          mkdir -p   /home/compression_tracer
          if [[ "$run_benchmark" == "run_wordcount" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/micro/sort/prepare
            ./my_prepare.sh $data_size
            sleep 1m
          fi
          if [[ "$run_benchmark" == "run_rf" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/ml/rf/prepare
            ./my_prepare.sh $data_size
            sleep 1m
          fi
          if [[ "$run_benchmark" == "run_svm" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/ml/svm/prepare
            ./my_prepare.sh $data_size
            sleep 1m
          fi
          if [[ "$run_benchmark" == "run_pagerank" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/websearch/pagerank/prepare
            ./my_prepare.sh $data_size
            sleep 1m
          fi
          if [[ "$run_benchmark" == "run_lda" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/ml/lda/prepare
            ./my_prepare.sh $data_size
            sleep 1m
          fi
          if [[ "$run_benchmark" == "run_logistic" ]]; then
            cd /opt/HiBench_compression/HiBench-master/bin/workloads/ml/lr/prepare
            ./my_prepare.sh $data_size
            sleep 1m
          fi
          for compression_algorithm in kaezip; do
		  for executor_num in 4; do
		     if [[ "$data_size" == "huge" ]]; then
		        executor_num=6
		     fi
		     for count in 1; do
		          delFiles
		          clear_cache
		          copyFiles
		          clean_monitor
		          start_monitor
		          $run_benchmark
		          clean_monitor
		          storage=E$executor_num-$count-$data_size-$compression_algorithm-$file_format-$run_benchmark$zlib_version
		          echo $storage
		          transFiles
		          #delFiles
		     done
		 done
         done
	 if [[ "$compression_write_opti" == "t" ]]; then
               echo "opti"
	       mv /home/compression_tracer /home/$run_benchmark-$data_size-opti
	       #tar -czvf /home/$run_benchmark-$data_size-opti.tar /home/$run_benchmark-$data_size-opti
               #sudo rm -r /home/$run_benchmark-$data_size-opti
	 fi
	 if [[ "$compression_write_opti" == "f" ]]; then
               echo "un-opti"
	       mv /home/compression_tracer /home/$run_benchmark-$data_size-unopti
	       #tar -czvf /home/$run_benchmark-$data_size-unopti.tar /home/$run_benchmark-$data_size-unopti
               #sudo rm -r /home/$run_benchmark-$data_size-unopti 
         fi
      done
           #rmData_ml
          #sleep 2m
      done

  done
}

#run_benchmark=run_sort
#data_size=bigdata
#generate_data

#sleep 10m 

#sync_time
#close_slaves
#close_master
#sleep 1m
#start_master
#start_slaves

rm -r /home/monitor/lda_temp_files/
mkdir -p /home/monitor/lda_temp_files

#sleep 1h
mkdir -p /tmp/spark-events

#$spark_home/bin/spark-submit --master spark://${nodes[0]}:7077 --class com.github.ehiggs.spark.terasort.TeraGen /home/paper_4_fgcs/spark-terasort-master/target/spark-terasort-1.2-SNAPSHOT-jar-with-dependencies.jar 500g hdfs://store-1:9000/HiBench/Terasort_in

test_zlib_others

#zlib_version=intel
#zlib_switch
shuffle_file_save=f
compression_flag=true
compression_algorithm=kaezip
compression_buffersize=32
compression_write_opti=f
compression_read_opti=f
performance_trace=f
mylogger_flag=f
raw_sample=f
executor_num=6


#ls -alt /usr/local/lib > $monitor_dir/libz_details
#test_compression_tracer



#run_sort
#run_sort
#run_wordcount
#run_kmeans
#run_lda
#run_linear
#run_logistic
#run_pagerank
#run_rf
#run_svm
#run_sqljoin
#test
#test_micro
