git pull
sbt package
time spark-submit --class NewInvIdx --num-executors 30 target/scala-2.10/*.jar Hw3/tmp Hw3/invidx
#rm merged.txt
#hdfs dfs -getmerge Hw2/pageranksp merged.txt
