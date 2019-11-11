# SparkJanusgraphLoader

So this may be a big heap of crap but it works. Currently only works with Janusgraph on Hbase with Solr. I didn't really need it to work for other graphs, backends, or indexing nor did I really want to test that, so didn't even bother including the other libs. The idea/need for me is something that can take orc files that each represent a type of vertex or edge from hdfs and shove it into a graph. Not a whole lot to it. I do plan on attempting to move the graph work off of the driver to the executors at some point.

--------------------------------------------------------------------------------------------------------------------------------
#A quick how to use at least as it stands now. My env includes kerberos and the code assumes the env has it as well. Not to teriable to rip it out if needed.

kinit moddingfox

classPath="/etc/hadoop/conf/core-site.xml,/etc/hbase/conf/hbase-site.xml"\
classPath=${classPath},$(echo $(ls target/*.jar) | tr " " ",")\
extraClassPath="target/guava-18.0.jar:/usr/hdp/current/spark2-client/jars/*"\
jar="target/Janusgraph_Ingestion.jar"

spark-submit \\\
--master yarn \\\
--class Entry \\\
--num-executors 5 \\\
--executor-cores 2 \\\
--driver-memory 16G \\\
--executor-memory 8G \\\
--conf spark.driver.extraClassPath=${extraClassPath} \\\
--conf spark.executor.extraClassPath=${extraClassPath} \\\
--jars ${classPath} \\\
${jar} \\\
--graphPropertiesFilePath=/home/moddingfox/JanusgraphSpark2Load/conf/janusgraph-hbase.properties \\\
--txCommitInterval=10000 \\\
--elementLabel=Equipment \\\
--srcPath=/tmp/mydb/Equipment_Vertex_Base \\\
--indexColumns=Equipment_ID \\\
--java.security.auth.login.config=/home/moddingfox/JanusgraphSpark2Load/conf/jaas.conf
