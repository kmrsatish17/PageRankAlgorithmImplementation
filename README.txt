Name: Satish Kumar
UNCC Id: 800966466

Please delete tempOutput and output folders to execute the code again.

Step 1: To do Initial Setup
	$ sudo su hdfs
	$ hadoop fs -mkdir /user/cloudera
	$ hadoop fs -chown cloudera /user/cloudera
	$ exit
	$ sudo su cloudera
	$ hadoop fs -mkdir /user/cloudera/pagerank /user/cloudera/pagerank/input

Step 2. Put all the input files into the new input directory
	
        $ hadoop fs -put input/* /user/cloudera/pagerank/input/

Step 3: To execute PageRank.java
	
        $ mkdir -p build
        $ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* PageRank.java -d build -Xlint
        $ jar -cvf pagerank.jar -C build/ .
        $ hadoop jar pagerank.jar org.myorg.PageRank /user/cloudera/pagerank/input /user/cloudera/pagerank/output
        $ hadoop fs -cat /user/cloudera/pagerank/output/*
        $ hadoop fs -get /user/cloudera/pagerank/output/part-r-00000 outputWikiMicro.txt


To execute again:-
        $ hadoop fs -rm -r /user/cloudera/pagerank
        $ hadoop fs -rm -r /home/cloudera/
	

EXTRA CREDIT IDEA:-

1) Wrtie a pass that determine whether or not Page Rank has converged rather than using a Fixed number of iteration.

To implement this, we will execute the iterative Map - Reduce job for a large number of time. In each iteration under the Reducer, we will save the previous page rank value for a key to a locally and then in the current iteration we will compare the value with the previous iteration page rank value. If we find any mismach then we will continue the iteration. If all are same that means our page rank value get saturated and we will exit the loop.

