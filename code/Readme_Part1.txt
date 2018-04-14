
Required: Eclipse

1) Import Part1 as a existing maven project in eclipse.

2) Include pom.xml and Part1-1.jar file while extracting.

3) Run pom.xml as a Maven project to generate the jar file again or you can directly copy the Part1-1.jar into cluster
   Note: Part1-1.jar can be  found in axm163631_lxp160730_assignment1/Part1/target

4) Execute the jar file using below command in cluster(In the below command axm163631 is the netid."hdfs://cshadoop1/user/axm163631/assignment1/" is the destination path where files will be saved):

hadoop jar Part1-1.jar Part1.Part1.Assignment1_Part1 hdfs://cshadoop1/user/axm163631/assignment1/

Note: assignment1 folder should be created manually if it does not exist before executing the above command.

5) Output can be verified by executing below command:

hdfs dfs -ls /user/axm163631/assignment1

All the files downloaded and decompressed will be displayed. Note: Compressed files will be deleted.