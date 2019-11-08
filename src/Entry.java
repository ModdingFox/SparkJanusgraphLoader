import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Entry
{	
	private static final String AppName = "Janusgraph_Ingestion";
	private static final Logger log = LoggerFactory.getLogger(Entry.class);
	
	private static String graphPropertiesFilePath = null;
	private static int txCommitInterval = 1000;
	private static String elementLabel = null;
	private static String srcPath = null;
	private static List<String> indexColumns = new ArrayList<String>();
	
	private static Boolean initMode = false;
	private static Boolean vertexMode = false;
	
    private static String primaryKeyColumn = null;
    private static String primaryKeyMapSaveLocation;
    
	
	private static String edgeKeyA = null;
	private static String edgeKeyB = null;
	private static String vertexLabelA = null;
	private static String vertexLabelB = null;
	private static String vertexKeyA = null;
	private static String vertexKeyB = null;
	private static String vertexAPrimaryKeyMapLoadLocation = null;
	private static String vertexBPrimaryKeyMapLoadLocation = null;
	
    private static boolean Load_Agrs(String[] args, SparkContext sc)
    {
        Options options = new Options();

        options.addOption("HELP", false, "Displays this message");

        options.addOption("graphPropertiesFilePath", true, "Path to a graph properties file");
        options.addOption("txCommitInterval", true, "The number of records to commit in each transaction");
        options.addOption("elementLabel", true, "This is the graph element vertex/edge label");
        options.addOption("srcPath", true, "Path to data on hdfs");
        options.addOption("indexColumns", true, "A comma seperated column list to define what properties need indices created");
        options.addOption("java_security_auth_login_config", true, "Path to jaas.conf file");
        
        options.addOption("mode", true, "initVertex, Vertex, initEdge, or Edge");
        
        options.addOption("primaryKeyColumn", true, "Name of column to track key pk to vertex id mappings");
        options.addOption("primaryKeyMapSaveLocation", true, "Path to save pk to vertex id mappings");

        options.addOption("edgeKeyA", true, "The property name from edge a to use when searching for vertex a");
        options.addOption("edgeKeyB", true, "The property name from edge b to use when searching for vertex b");
        options.addOption("vertexLabelA", true, "The vertex label for vertex a. Used when matching edgeKeyA to vertexKeyA");
        options.addOption("vertexLabelB", true, "The vertex label for vertex b. Used when matching edgeKeyB to vertexKeyB");
        options.addOption("vertexKeyA", true, "The property name from vertex a to use when searching for vertex a");
        options.addOption("vertexKeyB", true, "The property name from vertex b to use when searching for vertex b");
        
        options.addOption("vertexAPrimaryKeyMapLoadLocation", true, "PK to graph id map for vertex A");
        options.addOption("vertexBPrimaryKeyMapLoadLocation", true, "PK to graph id map for vertex B");
    
        CommandLineParser parser = new PosixParser();
        try
        {
            CommandLine cmd = parser.parse(options, args);
            HelpFormatter formatter = new HelpFormatter();

            if(cmd.hasOption("HELP"))
            {
                    formatter.printHelp(AppName, options);
                    return false;
            }
            
            if(cmd.hasOption("graphPropertiesFilePath")) { graphPropertiesFilePath = cmd.getOptionValue("graphPropertiesFilePath"); }
            else
            {
            	log.error("Missing commandline params: graphPropertiesFilePath required");
            	formatter.printHelp(AppName, options);
                return false;
            }
            
            if(cmd.hasOption("txCommitInterval"))
            { 
            	try
            	{ 
            		txCommitInterval = Integer.parseInt(cmd.getOptionValue("txCommitInterval"));
            		
            		if(txCommitInterval <= 0)
            		{
            			log.error("Bad commandline params: txCommitInterval must be a positive number greater than 0");
                    	formatter.printHelp(AppName, options);
                        return false;
            		}
            	}
            	catch(NumberFormatException e)
            	{
            		log.error("Bad commandline params: txCommitInterval must be a number", e);
                	formatter.printHelp(AppName, options);
                    return false;
            	}
            }
            
            if(cmd.hasOption("elementLabel")) { elementLabel = cmd.getOptionValue("elementLabel"); }
            else
            {
            	log.error("Missing commandline params: elementLabel required");
            	formatter.printHelp(AppName, options);
                return false;
            }
            
            if(cmd.hasOption("srcPath")) { srcPath = cmd.getOptionValue("srcPath"); }
            else
            {
            	log.error("Missing commandline params: srcPath required");
            	formatter.printHelp(AppName, options);
                return false;
            }
            
            if(cmd.hasOption("indexColumns"))
            {
            	indexColumns.addAll(Arrays.asList(cmd.getOptionValue("indexColumns").split(",")));
            }
            
            if(cmd.hasOption("java_security_auth_login_config")) { System.setProperty("java.security.auth.login.config", cmd.getOptionValue("java_security_auth_login_config")); }
            else
            {
            	log.error("Missing commandline params: java_security_auth_login_config required");
            	formatter.printHelp(AppName, options);
                return false;
            }
            
            if(cmd.hasOption("mode"))
            {
            	if(cmd.getOptionValue("mode").equals("Vertex"))
            	{
            	    vertexMode = true;
            	    
            	    if(cmd.hasOption("primaryKeyColumn")) { primaryKeyColumn = cmd.getOptionValue("primaryKeyColumn"); }
                    else
                    {
                    	log.error("Missing commandline params: primaryKeyColumn required when running in vertex mode");
                    	formatter.printHelp(AppName, options);
                        return false;
                    }
            	    
            	    if(cmd.hasOption("primaryKeyMapSaveLocation")) { primaryKeyMapSaveLocation = cmd.getOptionValue("primaryKeyMapSaveLocation"); }
                    else
                    {
                    	log.error("Missing commandline params: primaryKeyMapSaveLocation required when running in edge mode");
                    	formatter.printHelp(AppName, options);
                        return false;
                    }
            	}
            	else if(cmd.getOptionValue("mode").equals("initVertex"))
            	{ 
            		initMode = true;
            		vertexMode = true;
            	}
            	else if(cmd.getOptionValue("mode").equals("Edge"))
            	{
            		vertexMode = false;
            		
                    if(cmd.hasOption("edgeKeyA")) { edgeKeyA = cmd.getOptionValue("edgeKeyA"); }
                    else
                    {
                    	log.error("Missing commandline params: edgeKeyA required when running in edge mode");
                    	formatter.printHelp(AppName, options);
                        return false;
                    }
                    
                    if(cmd.hasOption("edgeKeyB")) { edgeKeyB = cmd.getOptionValue("edgeKeyB"); }
                    else
                    {
                    	log.error("Missing commandline params: edgeKeyB required when running in edge mode");
                    	formatter.printHelp(AppName, options);
                        return false;
                    }
                    
                    if(cmd.hasOption("vertexLabelA")) { vertexLabelA = cmd.getOptionValue("vertexLabelA"); }
                    else
                    {
                    	log.error("Missing commandline params: vertexLabelA required when running in edge mode");
                    	formatter.printHelp(AppName, options);
                        return false;
                    }
                    
                    if(cmd.hasOption("vertexLabelB")) { vertexLabelB = cmd.getOptionValue("vertexLabelB"); }
                    else
                    {
                    	log.error("Missing commandline params: vertexLabelB required when running in edge mode");
                    	formatter.printHelp(AppName, options);
                        return false;
                    }
                    
                    if(cmd.hasOption("vertexKeyA")) { vertexKeyA = cmd.getOptionValue("vertexKeyA"); }
                    else
                    {
                    	log.error("Missing commandline params: vertexKeyA required when running in edge mode");
                    	formatter.printHelp(AppName, options);
                        return false;
                    }
                    
                    if(cmd.hasOption("vertexKeyB")) { vertexKeyB = cmd.getOptionValue("vertexKeyB"); }
                    else
                    {
                    	log.error("Missing commandline params: vertexKeyB required when running in edge mode");
                    	formatter.printHelp(AppName, options);
                        return false;
                    }
                    
                    if(cmd.hasOption("vertexAPrimaryKeyMapLoadLocation")) { vertexAPrimaryKeyMapLoadLocation = cmd.getOptionValue("vertexAPrimaryKeyMapLoadLocation"); }
                    else
                    {
                    	log.error("Missing commandline params: vertexAPrimaryKeyMapLoadLocation required when running in edge mode");
                    	formatter.printHelp(AppName, options);
                        return false;
                    }
                    
                    if(cmd.hasOption("vertexBPrimaryKeyMapLoadLocation")) { vertexBPrimaryKeyMapLoadLocation = cmd.getOptionValue("vertexBPrimaryKeyMapLoadLocation"); }
                    else
                    {
                    	log.error("Missing commandline params: vertexBPrimaryKeyMapLoadLocation required when running in edge mode");
                    	formatter.printHelp(AppName, options);
                        return false;
                    }              
            	}
            	else if(cmd.getOptionValue("mode").equals("initEdge")) { initMode = true; }
            	else
            	{
            		log.error("Bad commandline params: mode must be etiher initVertex, Vertex, initEdge, or Edge");
                	formatter.printHelp(AppName, options);
                    return false;
            	}
            }
            else
            {
            	log.error("Missing commandline params: mode required");
            	formatter.printHelp(AppName, options);
                return false;
            }

            return true;
        }
        catch (ParseException e) { log.error("ParseException", e); }

        return false;
    }
	
	public static void main(String[] args)
	{
		SparkSession spark = SparkSession.builder().appName(AppName).getOrCreate();
		SparkContext sc = spark.sparkContext();

        if(Load_Agrs(args, sc))
        {
            Configuration conf = new Configuration();
    		conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            try { UserGroupInformation.loginUserFromSubject(null); }
            catch (IOException e)
            {
                String Message = "Could not get login form user subject";
                log.error(Message, e);
                throw new SecurityException(Message);
            }
            
            graphCreate graphBuilder = new graphCreate(graphPropertiesFilePath, spark, txCommitInterval);
            
            if(vertexMode)
            {
            	if(initMode)
            	{
            		if(!graphBuilder.initGraphVertexFromOrc(elementLabel, srcPath, indexColumns)) { System.exit(-1); } 
            	}
            	else
            	{
            		if(!graphBuilder.loadVerticiesFromOrc(elementLabel, srcPath, primaryKeyColumn, primaryKeyMapSaveLocation)) { System.exit(-1); } 
            	}
            }
            else
            {
            	if(initMode)
            	{
            		if(!graphBuilder.initGraphEdgeFromOrc(elementLabel, srcPath)) { System.exit(-1); } 
            	}
            	else
            	{
            		if(!graphBuilder.loadEdgesFromOrc(elementLabel, srcPath, edgeKeyA, edgeKeyB, vertexLabelA, vertexLabelB, vertexKeyA, vertexKeyB, vertexAPrimaryKeyMapLoadLocation, vertexBPrimaryKeyMapLoadLocation)) { System.exit(-1); }
            	}
            }
            
            graphBuilder.closeGraph();
        }
        else { System.exit(-1); }
        
        spark.close();
    }

}
