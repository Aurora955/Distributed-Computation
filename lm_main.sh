#!/bin/bash

PWD=$(cd $(dirname $0); pwd)
cd $PWD 1> /dev/null 2>&1

TASKNAME=task_car1LQH

HADOOP_OUTPUT_DIR=/211019lqh/lqhoutput_car

echo $HADOOP_OUTPUT_DIR

hadoop fs -rm -r $HADOOP_OUTPUT_DIR

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.1.3.jar \
-jobconf mapred.job.name=$TASKNAME \
-file $PWD/lm_map.py $PWD/lm_reducer.py \
-input /211019lqh/cleaned_data2.csv \
-output ${HADOOP_OUTPUT_DIR} \
-mapper "lm_map.py" \
-reducer "lm_reducer.py" \
-numReduceTasks 1

if [ $? -ne 0 ]; then
    echo 'error'
    exit 1
fi
hadoop fs -touchz ${HADOOP_OUTPUT_DIR}/done

exit 0
