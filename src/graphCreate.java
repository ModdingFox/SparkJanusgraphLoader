import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
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
    
    private Boolean createVertexPropertyIndex(String propertyName, String indexPrefix)
    {
        log.info("Creating vertex index: " + propertyName);
        graph.tx().rollback();
        
        ManagementSystem mgmt = (ManagementSystem)graph.openManagement();
        PropertyKeyVertex propertyKey = (PropertyKeyVertex)mgmt.getPropertyKey(propertyName);
        
        String indexName = indexPrefix + "_Vertex_" + propertyName;
        
        if (propertyKey.dataType() == String.class) { mgmt.buildIndex(indexName, Vertex.class).addKey(propertyKey, Mapping.TEXT.asParameter()).buildMixedIndex("search"); }
        else { mgmt.buildIndex(indexName, Vertex.class).addKey(propertyKey).buildMixedIndex("search"); }
        
        mgmt.commit();
        graph.tx().rollback();
        
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
        
        log.info("Created vertex index: " + propertyName);
        
        return true;
    }

    private void createGraphProperty(String propertyName, Class propertyClass, Cardinality propertyCardinality)
    {
        log.info("Checking for PropertyKey: " + propertyName);
        
        ManagementSystem mgmt = (ManagementSystem)graph.openManagement();
        
        if(mgmt.getPropertyKey(propertyName) == null)
        {
            log.info("Creating PropertyKey: " + propertyName);  
            mgmt.makePropertyKey(propertyName).dataType(propertyClass).cardinality(propertyCardinality).make();
            mgmt.commit();
            log.info("Created PropertyKey: " + propertyName);   
        }
        else { log.info("PropertyKey already exists: " + propertyName); }
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
    
    public boolean loadVerticiesFromOrc(String vertexLabel, String srcPath, List<String> indexColumns)
    {   
        createVertex(vertexLabel);
        Dataset<Row> srcData = spark.read().orc(srcPath);
        List<Tuple2<String, String>> srcDataColumns = Arrays.asList(srcData.dtypes());
        
        String graphStoreManagerName = graph.getBackend().getStoreManager().getName();
        
        for(Tuple2<String, String> srcDataColumn : srcDataColumns)
        {
            createGraphProperty(srcDataColumn._1, convertTypesFromSparkToJanusgraph(srcDataColumn._2), Cardinality.SINGLE);
            if(indexColumns.contains(srcDataColumn._1)) { createVertexPropertyIndex(srcDataColumn._1, graphStoreManagerName); }
        }
        
        JanusGraphTransaction tx = graph.newTransaction();
        int txCounter = 0;
        int txCount = 0;
        
        for(Row currentRow : srcData.collectAsList())
        {
            JanusGraphVertex newVertex = tx.addVertex(T.label, vertexLabel);
            
            for(int column = 0; column < srcDataColumns.size(); column++)
            {
                Object newPropertyValue = getColumnData(srcDataColumns, column, currentRow);
                if(newPropertyValue != null)
                {
                    newVertex.property(srcDataColumns.get(column)._1, newPropertyValue);
                }
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
    
    public boolean loadEdgesFromOrc(String edgeLabel, String srcPath, String edgeKeyA, String edgeKeyB, String vertexLabelA, String vertexLabelB, String vertexKeyA, String vertexKeyB)
    {
        createEdge(edgeLabel);
        Dataset<Row> srcData = spark.read().orc(srcPath);
        List<Tuple2<String, String>> srcDataColumns = Arrays.asList(srcData.dtypes());
        
        String graphStoreManagerName = graph.getBackend().getStoreManager().getName();
        
        int edgeKeyAColumnId = -1;
        int edgeKeyBColumnId = -1;
        
        for(Tuple2<String, String> srcDataColumn : srcDataColumns)
        {
            createGraphProperty(srcDataColumn._1, convertTypesFromSparkToJanusgraph(srcDataColumn._2), Cardinality.SINGLE);
            
            if(srcDataColumn._1.equals(edgeKeyA)) { edgeKeyAColumnId = srcDataColumns.indexOf(srcDataColumn); }
            if(srcDataColumn._1.equals(edgeKeyB)) { edgeKeyBColumnId = srcDataColumns.indexOf(srcDataColumn); }
        }
        
        if(edgeKeyAColumnId == -1)
        {
            log.error("Could not find edgeKeyA in src data");
            return false;
        }
        
        if(edgeKeyBColumnId == -1)
        {
            log.error("Could not find edgeKeyB in src data");
            return false;
        }
        
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
        
        for(Row currentRow : srcData.collectAsList())
        {
            Object edgeKeyAPropertyValue = getColumnData(srcDataColumns, edgeKeyAColumnId, currentRow);
            Object edgeKeyBPropertyValue = getColumnData(srcDataColumns, edgeKeyBColumnId, currentRow);
            
            if(edgeKeyAPropertyValue != null && edgeKeyBPropertyValue != null)
            {
                Vertex vertexA = null;
                Vertex vertexB = null;
                
                if(edgeKeyAPropertyValue instanceof java.lang.String) { vertexA = g.V().has(vertexKeyA, org.janusgraph.core.attribute.Text.textContains(edgeKeyAPropertyValue)).has(T.label, vertexLabelA).next(); }
                else { vertexA = g.V().has(vertexKeyA, edgeKeyAPropertyValue).has(T.label, vertexLabelA).next(); }
                
                
                if(edgeKeyBPropertyValue instanceof java.lang.String) { vertexB = g.V().has(vertexKeyB, org.janusgraph.core.attribute.Text.textContains(edgeKeyBPropertyValue)).has(T.label, vertexLabelB).next(); }
                else { vertexB = g.V().has(vertexKeyB, edgeKeyBPropertyValue).has(T.label, vertexLabelB).next(); } 
                
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
    
}
