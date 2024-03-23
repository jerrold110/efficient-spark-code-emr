sudo yum install tmux -y
tmux new -s spark
wget https://www.dropbox.com/s/3uo4gznau7fn6kg/Archive.zip
unzip Archive.zip
hdfs dfs -ls / # list all the HDFS folder
hdfs dfs -mkdir /input # make a directory called input in HDFS
hdfs dfs -copyFromLocal  2015.csv /input # copy data from local Filesystem to HDFS
hdfs dfs -copyFromLocal  2016.csv /input
hdfs dfs -ls /input # check to see your copied data

wget https://www.dropbox.com/s/yuw9m5dbg03sad8/plate_type.csv
hdfs dfs -mkdir /mapping
hdfs dfs -copyFromLocal  plate_type.csv /mapping
hdfs dfs -ls /mapping


# For joins
hdfs dfs -rm -r /output # free up some space in HDFS
pyspark --num-executors=2 # start pyspark shell