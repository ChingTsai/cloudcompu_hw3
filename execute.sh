# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/WordCount/Output/
#hadoop jar WordCount.jar wordcount.WordCount /user/shared/WordCount/Input /user/TA/WordCount/Output
#hdfs dfs -cat /user/TA/WordCount/Output/part-*

hdfs dfs -rm -r input
hdfs dfs -put input input
hdfs dfs -rm -r invidx
hadoop jar ${PWD##*/}.jar CloudCompu.hw1.InvertedIndex input invidx
hdfs dfs -cat invidx/part-*
rm -rf invidx
hdfs dfs -get invidx .
