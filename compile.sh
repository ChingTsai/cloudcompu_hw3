# You do not have to modify this file unless you have multiple packages in your MR java code
printf "Cleaning old src ...\n"
rm -r code/*
cp query/src/main/java/hw3/query/* code 
printf "Cleaning old class ...\n"
rm -r class/*
printf "Java Compile ... \n"
javac -Xlint -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.7.2.jar:$HBASE_HOME/lib/hbase-common-1.2.1.jar:$HBASE_HOME/lib/hbase-client-1.2.1.jar -d class code/*
printf "packaging ... \n"
jar -cvf Query.jar -C class/ .
printf "Done!\n"
