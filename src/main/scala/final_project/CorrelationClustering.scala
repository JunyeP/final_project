package final_project

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI

object CorrelationClustering {

  /* ------------------------------------------------------------------------
   *  helper: deterministic hash‑rank  U[0,1)
   * --------------------------------------------------------------------- */
  private def hashRank(id: Long, seed: Long): Double = {
    val h = scala.util.hashing.MurmurHash3.stringHash(s"$seed:$id") & 0x7FFFFFFFL
    h.toDouble / Int.MaxValue              // 0 ≤ h < 1
  }

  /* vertex attribute used internally */
  private case class VAttr(rank: Double, bucket: Int, cid: Long)

  /* encode edge sign in one byte */
  private def toSign(w: Double): Byte = if (w >= 0.0) 1.toByte else (-1).toByte

  /* ------------------------------------------------------------------------
   *  MAIN: distributed correlation‑clustering
   * --------------------------------------------------------------------- */
  def correlationClustering(
      rawGraph: Graph[Int, Double],
      trials:   Int    = 5,                // # permutation trials (quality booster)
      alpha:    Double = 2.0 / 13.0,       // α constant from the paper
      seed:     Long   = 42L)
     (implicit sc: SparkContext): Graph[Long, Double] = {

    // ─── constant pre‑processing ─────────────────────────────────────────
    val g0: Graph[Int, Byte] = rawGraph
      .mapEdges(e => toSign(e.attr))                       // +1 / –1
      .partitionBy(PartitionStrategy.EdgePartition2D)
      .cache()

    val L = math.ceil(1.0 / alpha).toInt                  // # rank buckets

    var bestCost = Long.MaxValue
    var bestCid:  VertexRDD[Long] = null

    // ─── outer "boosting" loop (independent permutations) ───────────────
    for (t <- 0 until trials) {

      /* 0 ▷ initialise ranks, buckets, singleton cluster‑ids  */
      val seeded = g0.mapVertices { case (vid, attr: Int) =>
        val r = hashRank(vid, seed + t)
        VAttr(rank = r,
              bucket = (r / alpha).toInt,
              cid = vid)                                  // start as own cluster
      }.cache()

      /* 1 ▷ α‑bucket rounds */
      val clustered: Graph[VAttr, Byte] =
        (0 until L).foldLeft(seeded) { (g, bIdx) =>

          // 1a — find active singletons in bucket bIdx
          val active = g.vertices.filter {
            case (vid, VAttr(_, b, cid)) => b == bIdx && cid == vid  // singleton check
          }.mapValues(_ => true)
          
          // Collect active vertices as a set, to avoid lookup()
          val activeVertices = active.keys.collect().toSet

          // quick exit if no active vertices in this bucket
          if (activeVertices.isEmpty) g else {

            // 1b — gain(S,C) aggregation  (only edges whose src in ACTIVE)
            // Use join instead of lookup to avoid the SparkContext issue
            val gains: RDD[((Long, Long), (Long, Long))] =
              g.triplets
               .filter(tr => activeVertices.contains(tr.srcId))  // Use contains instead of lookup
               .map { tr =>
                 val plus  = if (tr.attr > 0) 1L else 0L
                 val minus = if (tr.attr < 0) 1L else 0L
                 ((tr.srcAttr.cid, tr.dstAttr.cid), (plus, minus))
               }
               .reduceByKey { case ((g1,b1), (g2,b2)) => (g1 + g2, b1 + b2) }

            // 1c — pick profitable destination per source singleton
            val decisions: Map[Long, Long] = gains
              .mapValues { case (good, bad) => good - bad }    // net gain
              .filter(_._2 >= 0)
              .map { case ((src, dst), gain) => (src, (dst, gain)) }
              .reduceByKey { case ((d1,gain1), (d2,gain2)) =>
                if (gain1 > gain2 || (gain1 == gain2 && d1 < d2)) (d1, gain1) else (d2, gain2)
              }
              .mapValues(_._1)                                 // keep only dest cid
              .collectAsMap()

            // no merges in this bucket
            if (decisions.isEmpty) g

            else {
              val bcDec = g.vertices.sparkContext.broadcast(decisions)

              // 1d — apply merges  (one union‑find step)
              val gNext = g.mapVertices { case (vid, attr) =>
                val newCid = bcDec.value.getOrElse(attr.cid, attr.cid)
                if (newCid == attr.cid) attr else attr.copy(cid = newCid)
              }.cache()

              // DO NOT destroy the broadcast variable here!
              // Let Spark handle the cleanup of broadcast variables automatically

              g.unpersist(blocking = false)
              gNext
            }
          }
        }

      /* 2 ▷ cost for this trial */
      val cost = clustered.triplets.map { tr =>
        val same = tr.srcAttr.cid == tr.dstAttr.cid
        if ( same && tr.attr < 0 || !same && tr.attr > 0 ) 1L else 0L
      }.sum().toLong

      if (cost < bestCost) {
        bestCost = cost
        if (bestCid != null) bestCid.unpersist(blocking = false)
        bestCid = clustered.vertices.mapValues(_.cid).cache()
      }
      clustered.unpersist(blocking = false)
    }

    // ─── 3 ▷  return best clustering on original graph  ────────────────
    rawGraph.outerJoinVertices(bestCid) { (_, _, optCid) => optCid.get }
  }

  /* helper: vertex is singleton iff cid == vid  */
  private def vidFromAttr(cid: Long): Long = cid        // alias for clarity
  
  /* Main method to run the correlation clustering on input data */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: final_project.CorrelationClustering <input file> <output directory>")
      System.exit(1)
    }
    
    val inputFile = args(0)
    val outputDir = args(1)
    
    // Initialize Spark
    val conf = new SparkConf().setAppName("Correlation Clustering")
    val sc = new SparkContext(conf)
    
    println(s"Processing input file: $inputFile")
    println(s"Output will be saved to: $outputDir")
    
    try {
      // FIX: Get the correct filesystem for the output path
      val hadoopConf = sc.hadoopConfiguration
      val outputPath = new Path(outputDir)
      val fs = FileSystem.get(outputPath.toUri(), hadoopConf)
      
      // Delete output directory if it exists
      if (fs.exists(outputPath)) {
        println(s"Output directory $outputDir already exists. Deleting it.")
        fs.delete(outputPath, true)
      }
      
      // Read and parse the CSV file
      // Assuming the format is: source_id,target_id[,weight]
      val edges = sc.textFile(inputFile).map { line =>
        val fields = line.trim.split(",")
        val src = fields(0).toLong
        val dst = fields(1).toLong
        // If weight is provided, use it; otherwise default to 1.0 (positive edge)
        val weight = if (fields.length > 2) fields(2).toDouble else 1.0
        (src, dst, weight)
      }
      
      // Create the graph
      val vertexRDD: RDD[(VertexId, Int)] = edges.flatMap(e => 
        Iterator((e._1, 1), (e._2, 1))
      ).distinct()
      
      val edgeRDD: RDD[Edge[Double]] = edges.map(e => 
        Edge(e._1, e._2, e._3)
      )
      
      val graph = Graph(vertexRDD, edgeRDD)
      
      // Run correlation clustering
      val clusteredGraph = correlationClustering(
        graph,
        trials = 5,    // Number of trials
        alpha = 2.0/13.0,  // Alpha parameter from the paper
        seed = 42L    // Random seed
      )(sc)
      
      // For Dataproc, use Spark's saveAsTextFile instead of manual file writing
      // This avoids file system compatibility issues
      val clusters = clusteredGraph.vertices.map { case (vid, cid) => 
        s"$vid,$cid"
      }
      
      // Save results using saveAsTextFile
      clusters.saveAsTextFile(outputDir)
      
      // Print summary
      val clusterCount = clusteredGraph.vertices.map(_._2).distinct().count()
      println(s"Correlation clustering completed with $clusterCount clusters")
      
    } catch {
      case e: Exception => 
        println(s"Error processing file: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      sc.stop()
    }
  }
}