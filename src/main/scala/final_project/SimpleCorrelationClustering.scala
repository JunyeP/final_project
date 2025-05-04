package final_project

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.collection.mutable

object SimpleCorrelationClustering {
  
  // Main function to perform correlation clustering with random pivot selection
  def correlationClustering(
      graph: Graph[Int, Double], 
      seed: Long = 42L): Graph[Long, Double] = {
    
    val sc = graph.vertices.sparkContext
    
    // Setup random number generator for pivot selection
    val random = new Random(seed)
    
    // Initialize vertex status: 0 = UNCLUSTERED, 1 = CLUSTERED
    var workingGraph = graph.mapVertices((_, _) => 0).cache()
    
    // Track cluster assignments
    val clusterAssignments = mutable.Map[Long, Long]()
    
    // Keep track of unclustered vertices
    var unclusteredVertices = workingGraph.vertices
      .filter { case (_, status) => status == 0 }
      .map(_._1)
      .collect()
      .toSeq
    
    println(s"Starting clustering with ${unclusteredVertices.size} vertices")
    
    // Main clustering loop - while unclustered vertices remain
    while (unclusteredVertices.nonEmpty) {
      // Choose a random pivot from unclustered vertices
      val pivotIndex = random.nextInt(unclusteredVertices.size)
      val pivotId = unclusteredVertices(pivotIndex)
      
      // Create new cluster with ID same as pivot
      val clusterId = pivotId
      
      // Mark pivot as clustered and assign to its cluster
      clusterAssignments(pivotId) = clusterId
      
      // Find unclustered neighbors with positive edges to pivot
      val neighbors = workingGraph.edges
        .filter(e => 
          (e.srcId == pivotId || e.dstId == pivotId) && 
          e.attr > 0.0 // Only positive edges
        )
        .map(e => if (e.srcId == pivotId) e.dstId else e.srcId)
        .collect()
        .toSet
      
      // Get vertices that are both neighbors and unclustered
      val unclusteredNeighbors = neighbors.intersect(unclusteredVertices.toSet)
      
      // Add all unclustered neighbors to the same cluster
      for (neighborId <- unclusteredNeighbors) {
        clusterAssignments(neighborId) = clusterId
      }
      
      // Update vertex status to mark pivot and added vertices as clustered
      val newClusteredVertices = clusterId +: unclusteredNeighbors.toSeq
      val clusteredVerticesRDD = sc.parallelize(
        newClusteredVertices.map(id => (id, 1)) // Status 1 = CLUSTERED
      )
      
      // Update working graph with newly clustered vertices
      workingGraph = workingGraph.outerJoinVertices(clusteredVerticesRDD) {
        case (_, oldStatus, Some(newStatus)) => newStatus
        case (_, oldStatus, None) => oldStatus
      }.cache()
      
      // Update unclustered vertices list
      unclusteredVertices = workingGraph.vertices
        .filter { case (_, status) => status == 0 }
        .map(_._1)
        .collect()
        .toSeq
      
      println(s"Remaining unclustered vertices: ${unclusteredVertices.size}")
    }
    
    // Create final graph with cluster assignments
    val clusterAssignmentsRDD = sc.parallelize(clusterAssignments.toSeq)
    
    graph.outerJoinVertices(clusterAssignmentsRDD) {
      case (vid, _, Some(clusterId)) => clusterId
      case (vid, _, None) => vid // Fallback: assign to own cluster (shouldn't happen)
    }
  }
  
  // Utility method to load data from CSV into a GraphX graph
  def loadGraphFromCSV(sc: SparkContext, filePath: String): Graph[Int, Double] = {
    // Read the CSV file
    val edgeLines = sc.textFile(filePath)
    
    // Parse edges assuming format: srcId,dstId
    val edges = edgeLines.filter(_.trim.nonEmpty).map { line =>
      val fields = line.split(",")
      // Use 1.0 as default edge weight to indicate positive correlation
      Edge(fields(0).toLong, fields(1).toLong, 1.0)
    }
    
    // Create the graph with default vertex attributes
    Graph.fromEdges(edges, 0)
  }
  
  // Calculate clustering quality
  def calculateClusteringQuality(graph: Graph[Long, Double]): Double = {
    // Get all cluster IDs
    val clusterIds = graph.vertices.map(_._2).distinct().collect()
    
    // Calculate agreements (positive edges within clusters, negative edges between clusters)
    val agreements = graph.triplets.map { triplet =>
      val inSameCluster = triplet.srcAttr == triplet.dstAttr
      val edgeWeight = triplet.attr
      
      if (inSameCluster && edgeWeight > 0) {
        // Agreement: positive edge within cluster
        1.0
      } else if (!inSameCluster && edgeWeight < 0) {
        // Agreement: negative edge between clusters
        1.0
      } else {
        // Disagreement
        0.0
      }
    }.sum()
    
    // Total number of edges
    val totalEdges = graph.edges.count()
    
    // Calculate quality as ratio of agreements to total edges
    if (totalEdges == 0) 0.0 else agreements / totalEdges
  }
  
  // Main method
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimpleCorrelationClustering").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    // Parse command line arguments
    if (args.length < 2) {
      println("Usage: SimpleCorrelationClustering <input_file> <output_dir>")
      System.exit(1)
    }
    
    val inputFile = args(0)
    val outputDir = args(1)
    
    // Check if output directory exists and delete it
    val outputPath = new org.apache.hadoop.fs.Path(outputDir)
    val hdfs = outputPath.getFileSystem(sc.hadoopConfiguration)
    if (hdfs.exists(outputPath)) {
      println(s"Output directory $outputDir already exists, deleting it...")
      hdfs.delete(outputPath, true)
    }
    
    println(s"Loading graph from $inputFile")
    val inputGraph = loadGraphFromCSV(sc, inputFile)
    
    // Print graph statistics
    val vertexCount = inputGraph.vertices.count()
    val edgeCount = inputGraph.edges.count()
    println(s"Graph has $vertexCount vertices and $edgeCount edges")
    
    // Run correlation clustering
    println("Starting correlation clustering...")
    val clusteredGraph = correlationClustering(inputGraph)
    
    // Evaluate the clustering quality
    val quality = calculateClusteringQuality(clusteredGraph)
    println(s"Clustering Quality: $quality")
    
    // Count the number of clusters
    val clusterCount = clusteredGraph.vertices.map(_._2).distinct().count()
    println(s"Number of clusters: $clusterCount")
    
    // Print cluster distribution (size of each cluster)
    println("Cluster Distribution:")
    clusteredGraph.vertices
      .map(_._2)
      .countByValue()
      .toSeq
      .sortBy(-_._2)
      .take(10)
      .foreach { case (clusterId, count) =>
        println(s"Cluster $clusterId: $count vertices")
      }
    
    // Save the results in clean CSV format
    clusteredGraph.vertices.map { case (vertexId, clusterId) => 
      s"$vertexId,$clusterId"
    }.coalesce(1).saveAsTextFile(outputDir)
    
    sc.stop()
  }
}