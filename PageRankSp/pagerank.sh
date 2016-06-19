git pull
sbt package
time spark-submit --class NewPageRank --num-executors 30 target/scala-2.10/*.jar /shared/HW2/sample-in/input-100M Hw3/PageRank
#rm merged.txt
#hdfs dfs -getmerge Hw2/pageranksp merged.txt
