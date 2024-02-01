#!/bin/bash

"""
spark_source_dir=/home/zcp/spark_experiments/my_spark-orig_scheduler/spark-3.0.1

cd $spark_source_dir

./dev/make-distribution.sh --name my_spark-orig_scheduler --pip --tgz \
                           -Phadoop-2.10 -Dhadoop.version=2.10.1 \
                           #-Phive -Phive-thriftserver \
                           #-Pyarn -Pkubernetes

"""

#spark_source_dir=/home/zcp/spark_experiments/spark_KAE源码提交/spark-3.2.0/
spark_source_dir=~/spark_PAC/
#spark_source_dir=/home/zcp/spark_experiments/spark-3.1.2/

#spark_source_dir=/home/zcp/spark_experiments/spark-3.2.0_shuffle_files/
cd $spark_source_dir

./dev/make-distribution.sh --name spark_PAC --pip --tgz \
                           -Phadoop-3.2.0 -Phive -Pyarn
                           #-Phive -Phive-thriftserver \
                           #-Pyarn -Pkubernetes

