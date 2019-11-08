import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.types.vertices.PropertyKeyVertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

//need to add error handling

public class graphCreate
{
	private static final Logger log = LoggerFactory.getLogger(graphCreate.class);
	
    StandardJanusGraph graph = null;
    SparkSession spark = null;
    int txCommitInterval = 0;
    
    @SuppressWarnings("rawtypes")
	private Class convertTypesFromSparkToJanusgraph(String sparkType)
    {
    	switch (sparkType)
    	{
    		case "ByteType":
    			return Byte.class;
    		case "ShortType":
    			return Short.class;
    		case "IntegerType":
    			return Integer.class;
    		case "LongType":
    			return Long.class;
    		case "FloatType":
    			return Float.class;
    		case "DoubleType":
    			return Double.class;
    		case "StringType":
    			return String.class;
    		case "BooleanType":
    			return Date.class;
    		default:
    			return null;
    	}
    }
    
    private DataType convertStringToSparkType(String sparkType)
    {
    	switch (sparkType)
    	{
    		case "ByteType":
    			return DataTypes.ByteType;
    		case "ShortType":
    			return DataTypes.ShortType;
    		case "IntegerType":
    			return DataTypes.IntegerType;
    		case "LongType":
    			return DataTypes.LongType;
    		case "FloatType":
    			return DataTypes.FloatType;
    		case "DoubleType":
    			return DataTypes.DoubleType;
    		case "StringType":
    			return DataTypes.StringType;
    		case "BooleanType":
    			return DataTypes.BooleanType;
    		default:
    			return null;
    	}
    }
    
    private Object getColumnData(List<Tuple2<String, String>> srcDataColumns, int column, Row currentRow)
    {
    	if(currentRow.get(column) == null) { log.debug("Skipping column with null value " + srcDataColumns.get(column)._1); return null; }
        else if(srcDataColumns.get(column)._2 == "ByteType") { return currentRow.getByte(column); }
        else if(srcDataColumns.get(column)._2 == "ShortType") { return currentRow.getShort(column); }
        else if(srcDataColumns.get(column)._2 == "IntegerType") { return currentRow.getInt(column); }
        else if(srcDataColumns.get(column)._2 == "LongType") { return currentRow.getLong(column); }
        else if(srcDataColumns.get(column)._2 == "FloatType") { return currentRow.getFloat(column); }
        else if(srcDataColumns.get(column)._2 == "LongType") { return currentRow.getLong(column); }
        else if(srcDataColumns.get(column)._2 == "DoubleType") { return currentRow.getDouble(column); }
        else if(srcDataColumns.get(column)._2 == "StringType") { return currentRow.getString(column); }
        else if(srcDataColumns.get(column)._2 == "BooleanType") { return currentRow.getBoolean(column); }
        else if(srcDataColumns.get(column)._2 == "DateType") { return currentRow.getDate(column); }
        else { log.info("Skipping column " + srcDataColumns.get(column)._1 + " with data type " + srcDataColumns.get(column)._2); return null; }	
    }
    
    private void stopAllCurrentMGMTTx()
    {
        ManagementSystem mgmt = (ManagementSystem)graph.openManagement();
        
        graph.getOpenTransactions().forEach(tx -> tx.rollback());
        
        mgmt = (ManagementSystem)graph.openManagement();

        for(String it : mgmt.getOpenInstances())
        {
        	if(!it.endsWith("(current)")) { mgmt.forceCloseInstance(it); }
        }
        
        mgmt.commit();
    }
    
    private Boolean createVertexPropertyMixedIndex(String indexPrefix, String vertexLabel, List<String> indexColumns)
    {
    	String indexName = indexPrefix + "_Vertex_" + vertexLabel;
    	
    	log.info("Attempting vertex index create: " + indexName);
        graph.tx().rollback();
        
        stopAllCurrentMGMTTx();
        
        ManagementSystem mgmt = (ManagementSystem)graph.openManagement();
        
        if(mgmt.getGraphIndex(indexName) == null)
        { 
        	VertexLabel selectVertex = mgmt.getVertexLabel(vertexLabel);
            
            IndexBuilder newIndex = mgmt.buildIndex(indexName, Vertex.class).indexOnly(selectVertex);
            
            for(String indexColumn : indexColumns)
            {
            	log.info("Adding property to index: " + indexColumn);
            	PropertyKeyVertex propertyKey = (PropertyKeyVertex)mgmt.getPropertyKey(indexColumn);
            	if (propertyKey.dataType() == String.class) { newIndex = newIndex.addKey(propertyKey, Mapping.TEXT.asParameter()); }
                else { newIndex = newIndex.addKey(propertyKey); }
            }
            
            newIndex.buildMixedIndex("search");
            
            mgmt.commit();
            graph.tx().rollback();
            
            stopAllCurrentMGMTTx();
            
            try
            {
                mgmt.awaitGraphIndexStatus(graph, indexName).call();
                mgmt = (ManagementSystem)graph.openManagement();
                mgmt.updateIndex(mgmt.getGraphIndex(indexName), SchemaAction.REINDEX).get();
                mgmt.commit();
            } 
            catch (InterruptedException e)
            {
            	log.error("Failed to create vertex index: InterruptedException - " + indexName, e);
                return false;
    	    }
            catch (ExecutionException e)
            {
                log.error("Failed to create vertex index: ExecutionException - " + indexName, e);
                return false;
            }
        }
        else { log.info("Vertex index already exists: " + indexPrefix); }
        
        return true;
        
    }
    
    @SuppressWarnings("rawtypes")
	private void createGraphProperty(String propertyName, Class propertyClass, Cardinality propertyCardinality)
    {
    	log.info("Checking for PropertyKey: " + propertyName + " Type: " + propertyClass.getName());
    	
        ManagementSystem mgmt = (ManagementSystem)graph.openManagement();
        
        PropertyKey graphProperty = mgmt.getPropertyKey(propertyName);
        
    	if(graphProperty == null)
    	{
        	log.info("Creating PropertyKey: " + propertyName);  
            mgmt.makePropertyKey(propertyName).dataType(propertyClass).cardinality(propertyCardinality).make();
            mgmt.commit();
            log.info("Created PropertyKey: " + propertyName);	
    	}
    	else
    	{ 
    		if(graphProperty.dataType() == propertyClass) { log.info("PropertyKey already exists: " + propertyName); }
    		else { log.error("PropertyKey data type conflict: " + propertyName + " Found: " + graphProperty.dataType()); }
    	}
    }
    
    private void createVertex(String vertexName)
    {
    	log.info("Checking for VertexLabel: " + vertexName);
        ManagementSystem mgmt = (ManagementSystem)graph.openManagement();
        
        if(mgmt.getVertexLabel(vertexName) == null)
        {
            log.info("Creating VertexLabel: " + vertexName);
            mgmt.makeVertexLabel(vertexName).make();
            mgmt.commit();
            log.info("Created VertexLabel: " + vertexName);
        }
        else { log.info("VertexLabel already exists: " + vertexName); }
    }
    
    public boolean initGraphVertexFromOrc(String vertexLabel, String srcPath, List<String> indexColumns)
    {
    	createVertex(vertexLabel);
    	Dataset<Row> srcData = spark.read().orc(srcPath);
    	List<Tuple2<String, String>> srcDataColumns = Arrays.asList(srcData.dtypes());
    	
    	String graphStoreManagerName = graph.getBackend().getStoreManager().getName();
    	
    	for(Tuple2<String, String> srcDataColumn : srcDataColumns) { createGraphProperty(srcDataColumn._1, convertTypesFromSparkToJanusgraph(srcDataColumn._2), Cardinality.SINGLE); }
    	
        if(indexColumns.size() > 0) { createVertexPropertyMixedIndex(graphStoreManagerName, vertexLabel, indexColumns); }
    	
    	return true;
    }
        
    public boolean loadVerticiesFromOrc(String vertexLabel, String srcPath, String primaryKeyColumn, String primaryKeyMapSaveLocation)
    {	
    	Dataset<Row> srcData = spark.read().orc(srcPath);
    	List<Tuple2<String, String>> srcDataColumns = Arrays.asList(srcData.dtypes());
    	
    	JanusGraphTransaction tx = graph.newTransaction();
    	int txCounter = 0;
        int txCount = 0;
        
        long partitionCount = srcData.count()/txCommitInterval;
        
        srcData = srcData.repartition(Math.toIntExact(partitionCount));
        
        Iterator<Row> rowIterator = srcData.toLocalIterator();

        DataType primaryKeyDataType = convertStringToSparkType(srcDataColumns.get(srcData.schema().fieldIndex(primaryKeyColumn))._2);
        List<Row> primaryKeyMap = new ArrayList<Row>();
        List<org.apache.spark.sql.types.StructField> primaryKeyMapStructField=new ArrayList<org.apache.spark.sql.types.StructField>();
        primaryKeyMapStructField.add(DataTypes.createStructField(primaryKeyColumn, primaryKeyDataType, true));
        primaryKeyMapStructField.add(DataTypes.createStructField("vertexID", DataTypes.LongType, true));
        StructType primaryKeyStructType = DataTypes.createStructType(primaryKeyMapStructField);
        
    	while(rowIterator.hasNext())
    	{
    		Row currentRow = rowIterator.next();
    		
    		JanusGraphVertex newVertex = tx.addVertex(T.label, vertexLabel);
   		
    		for(int column = 0; column < srcDataColumns.size(); column++)
    		{
    			Object newPropertyValue = getColumnData(srcDataColumns, column, currentRow);
    			if(newPropertyValue != null)
    			{
    				newVertex.property(srcDataColumns.get(column)._1, newPropertyValue);
    			}
    			
    			if(srcDataColumns.get(column)._1.equals(primaryKeyColumn)) { primaryKeyMap.add(RowFactory.create(newPropertyValue, newVertex.longId())); }
    		}

    		txCounter = txCounter + 1;
    		
            if(txCounter > txCommitInterval)
            {      
            	txCounter = 0;
                txCount = txCount + 1;
                tx.commit();
                tx.close();
                tx = graph.newTransaction();
                log.info(vertexLabel + ": Vertex Commit #" + txCount);
            }
    	}
    	
    	if(tx.isOpen())
    	{
            txCount = txCount + 1;
            tx.commit();
            tx.close();	
            log.info(vertexLabel + ": Vertex Commit #" + txCount);
    	}
    	
    	spark.createDataFrame(primaryKeyMap, primaryKeyStructType).write().format("orc").mode(org.apache.spark.sql.SaveMode.Overwrite).orc(primaryKeyMapSaveLocation);
    	
    	return true;
    }
    
    private void createEdge(String edgeName)
    {
    	log.info("Checking for EdgeLabel: " + edgeName);
        ManagementSystem mgmt = (ManagementSystem)graph.openManagement();
        
        if(mgmt.getEdgeLabel(edgeName) == null)
        {
            log.info("Creating EdgeLabel: " + edgeName);
            mgmt.makeEdgeLabel(edgeName).make();
            mgmt.commit();
            log.info("Created EdgeLabel: " + edgeName);
        }
        else { log.info("EdgeLabel already exists: " + edgeName); }
    }
    
    public boolean initGraphEdgeFromOrc(String edgeLabel, String srcPath)
    {
    	createEdge(edgeLabel);
    	Dataset<Row> srcData = spark.read().orc(srcPath);
    	List<Tuple2<String, String>> srcDataColumns = Arrays.asList(srcData.dtypes());
    	    	
    	for(Tuple2<String, String> srcDataColumn : srcDataColumns) { createGraphProperty(srcDataColumn._1, convertTypesFromSparkToJanusgraph(srcDataColumn._2), Cardinality.SINGLE); }
    	
    	return true;
    }
    
    public boolean loadEdgesFromOrc(String edgeLabel, String srcPath, String edgeKeyA, String edgeKeyB, String vertexLabelA, String vertexLabelB, String vertexKeyA, String vertexKeyB, String vertexAPrimaryKeyMapLoadLocation, String vertexBPrimaryKeyMapLoadLocation)
    {
    	Dataset<Row> srcData = spark.read().orc(srcPath);
    	List<Tuple2<String, String>> srcDataColumns = Arrays.asList(srcData.dtypes());
    	
    	int edgeKeyAColumnId = srcData.schema().fieldIndex(edgeKeyA);
    	int edgeKeyBColumnId = srcData.schema().fieldIndex(edgeKeyB);
    	    	    	
    	JanusGraphTransaction tx = graph.newTransaction();
    	GraphTraversalSource g = graph.traversal();
    	int txCounter = 0;
        int txCount = 0;
        
        PropertyKey vertexKeyAPropertyKey = graph.getPropertyKey(vertexKeyA);
        PropertyKey edgeKeyAPropertyKey = graph.getPropertyKey(edgeKeyA);
        
        if(vertexKeyAPropertyKey.dataType() != edgeKeyAPropertyKey.dataType())
        {
        	log.error("Data type mismatch(Pair A): " + edgeKeyA + " and " + vertexKeyA);
        	return false;
        }
        
        PropertyKey vertexKeyBPropertyKey = graph.getPropertyKey(vertexKeyB);
        PropertyKey edgeKeyBPropertyKey = graph.getPropertyKey(edgeKeyB);
        
        if(vertexKeyBPropertyKey.dataType() != edgeKeyBPropertyKey.dataType())
        {
        	log.error("Data type mismatch(Pair B): " + edgeKeyB + " and " + vertexKeyB);
        	return false;
        }
        
        if(graph.getVertexLabel(vertexLabelA) == null)
        {
        	log.error("Vertex label " + vertexLabelA + " does not exist");
        	return false;
        }
        
        if(graph.getVertexLabel(vertexLabelB) == null)
        {
        	log.error("Vertex label " + vertexLabelB + " does not exist");
        	return false;
        }
        
        long partitionCount = srcData.count()/txCommitInterval;
        
        srcData = srcData.repartition(Math.toIntExact(partitionCount));
        
        Iterator<Row> rowIterator = srcData.toLocalIterator();
        
        HashMap<Object, Long> vertexKeyMapA = new HashMap<Object, Long>();
        HashMap<Object, Long> vertexKeyMapB = new HashMap<Object, Long>();
        
        Dataset<Row> srcDataVertexAPrimaryKeyMap = spark.read().orc(vertexAPrimaryKeyMapLoadLocation);
        List<Tuple2<String, String>> srcDataVertexAPrimaryKeyMapColumns = Arrays.asList(srcDataVertexAPrimaryKeyMap.dtypes());
        Iterator<Row> srcDataVertexAPrimaryKeyMapIterator = srcDataVertexAPrimaryKeyMap.toLocalIterator();
        
        while(srcDataVertexAPrimaryKeyMapIterator.hasNext())
    	{
    		Row currentRow = srcDataVertexAPrimaryKeyMapIterator.next();
    		vertexKeyMapA.put(getColumnData(srcDataVertexAPrimaryKeyMapColumns, 0, currentRow), currentRow.getLong(1));
    	}
        
        Dataset<Row> srcDataVertexBPrimaryKeyMap = spark.read().orc(vertexBPrimaryKeyMapLoadLocation);
        List<Tuple2<String, String>> srcDataVertexBPrimaryKeyMapColumns = Arrays.asList(srcDataVertexBPrimaryKeyMap.dtypes());
        Iterator<Row> srcDataVertexBPrimaryKeyMapIterator = srcDataVertexBPrimaryKeyMap.toLocalIterator();
        
        while(srcDataVertexBPrimaryKeyMapIterator.hasNext())
    	{
    		Row currentRow = srcDataVertexBPrimaryKeyMapIterator.next();
    		vertexKeyMapB.put(getColumnData(srcDataVertexBPrimaryKeyMapColumns, 0, currentRow), currentRow.getLong(1));
    	}
        
    	while(rowIterator.hasNext())
    	{
    		Row currentRow = rowIterator.next();
    	    
    		Object edgeKeyAPropertyValue = getColumnData(srcDataColumns, edgeKeyAColumnId, currentRow);
    		Object edgeKeyBPropertyValue = getColumnData(srcDataColumns, edgeKeyBColumnId, currentRow);
    		
			if(edgeKeyAPropertyValue != null && edgeKeyBPropertyValue != null)
			{
				Vertex vertexA = null;
				Vertex vertexB = null;
				
				if(vertexKeyMapA.containsKey(edgeKeyAPropertyValue)) { vertexA = g.V(vertexKeyMapA.get(edgeKeyAPropertyValue)).next(); }
				else
				{
					if(edgeKeyAPropertyValue instanceof java.lang.String) { vertexA = g.V().has(vertexKeyA, org.janusgraph.core.attribute.Text.textContains(edgeKeyAPropertyValue)).has(T.label, vertexLabelA).next(); }
					else { vertexA = g.V().has(vertexKeyA, edgeKeyAPropertyValue).has(T.label, vertexLabelA).next(); }
				}
				
				if(vertexKeyMapB.containsKey(edgeKeyBPropertyValue)) { vertexB = g.V(vertexKeyMapB.get(edgeKeyBPropertyValue)).next(); }
				else
				{
					if(edgeKeyBPropertyValue instanceof java.lang.String) { vertexB = g.V().has(vertexKeyB, org.janusgraph.core.attribute.Text.textContains(edgeKeyBPropertyValue)).has(T.label, vertexLabelB).next(); }
		    		else { vertexB = g.V().has(vertexKeyB, edgeKeyBPropertyValue).has(T.label, vertexLabelB).next(); } 
				}
	    		
				if(vertexA != null && vertexB != null)
				{
		    		Edge newEdge = vertexA.addEdge(edgeLabel, vertexB);
		    		
		    		for(int column = 0; column < srcDataColumns.size(); column++)
		    		{
		    			Object newPropertyValue = getColumnData(srcDataColumns, column, currentRow);
		    			if(newPropertyValue != null)
		    			{
		    				newEdge.property(srcDataColumns.get(column)._1, newPropertyValue);
		    			}
		    		}
		    		
		    		txCounter = txCounter + 1;
				}
				else { log.error("Could not find verticies for edge"); }
	    		
	            if(txCounter > txCommitInterval)
	            {
	            	txCounter = 0;
	                txCount = txCount + 1;
	                tx.commit();
	                tx.close();
	                tx = graph.newTransaction();
	                log.info(edgeLabel + ": Edge Commit #" + txCount);
	            }
			}
    	}
    	
    	if(tx.isOpen())
    	{
            txCount = txCount + 1;
    	    tx.commit();
            tx.close();	
            log.info(edgeLabel + ": Edge Commit #" + txCount);
    	}
    	
    	return true;
    }
    
    public graphCreate(String graphPropertiesFileLocation, SparkSession sparkIn, int txCommitIntervalIn)
    {
        graph = (StandardJanusGraph)JanusGraphFactory.open(graphPropertiesFileLocation);
        spark = sparkIn;
        txCommitInterval = txCommitIntervalIn;
    }
    
    public void closeGraph() { graph.close(); } 
}
