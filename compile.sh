# You do not have to modify this file unless you have multiple packages in your MR java code
printf "Cleaning old src ...\n"
rm -r code/*
cp hw1/src/main/java/CloudCompu/hw1/* code 
printf "Cleaning old class ...\n"
rm -r class/*
printf "Java Compile ... \n"
javac -classpath  $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-2.7.2.jar -d class code/*
printf "packaging ... \n"
jar -cvf ${PWD##*/}.jar -C class/ .
printf "Done!\n"
