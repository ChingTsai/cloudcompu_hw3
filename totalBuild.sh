export SPARK_CLASSPATH=$SPARK_CLASSPATH:/usr/local/hbase/lib/*
cd ./PageRankSp
git pull
sbt package
time spark-submit --class InvertedIdx --num-executors 30 target/scala-2.10/*.jar /shared/HW2/sample-in/input-1G Hw3/tmp
time spark-submit --class NewInvIdx --num-executors 30 target/scala-2.10/*.jar Hw3/tmp Hw3/invidx
time spark-submit --class NewPageRank --num-executors 30 target/scala-2.10/*.jar /shared/HW2/sample-in/input-1G Hw3/PageRank
cd ..
cp ./PageRankSp/N.txt .
#hbase shell ./hbaseShell.txt
#hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator='^' -Dimporttsv.columns=HBASE_ROW_KEY,text s104062587:preprocess Hw3/preprocess
#hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator="," -Dimporttsv.columns=HBASE_ROW_KEY,df,info s104062587:invidx Hw3/invidx
#hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator="|" -Dimporttsv.columns=HBASE_ROW_KEY,pr s104062587:pagerank Hw3/PageRank
#hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator="|" -Dimporttsv.columns=HBASE_ROW_KEY,title s104062587:ids2title Hw3/ids2title
#hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator="|" -Dimporttsv.columns=HBASE_ROW_KEY,id s104062587:title2ids Hw3/title2ids
