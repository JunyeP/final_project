package final_project

import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.conf.Configuration
import java.io.{BufferedWriter, OutputStreamWriter}
import scala.collection.mutable.ArrayBuffer

object MergeOutputFiles {
  
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: scala MergeOutputFiles <input_directory> <output_file>")
      System.exit(1)
    }
    
    val inputDir = args(0)
    val outputFile = args(1)
    
    println(s"Merging files from $inputDir to $outputFile")
    
    // Initialize Hadoop configuration
    val conf = new Configuration()
    val fs = FileSystem.get(new Path(inputDir).toUri, conf)
    
    try {
      // Check if input directory exists
      val inputPath = new Path(inputDir)
      if (!fs.exists(inputPath)) {
        throw new Exception(s"Input directory $inputDir does not exist")
      }
      
      // Get all part files
      val partFiles = ArrayBuffer[Path]()
      val fileStatuses = fs.listStatus(inputPath)
      
      for (status <- fileStatuses) {
        val fileName = status.getPath.getName
        if (fileName.startsWith("part-") && !fileName.endsWith(".crc")) {
          partFiles += status.getPath
        }
      }
      
      println(s"Found ${partFiles.size} part files to merge")
      
      // Create output file
      val outputPath = new Path(outputFile)
      if (fs.exists(outputPath)) {
        println(s"Output file $outputFile already exists. Deleting it.")
        fs.delete(outputPath, false)
      }
      
      val outputStream = fs.create(outputPath)
      val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
      
      // Merge files
      for (partFile <- partFiles) {
        println(s"Processing file: ${partFile.getName}")
        
        val inputStream = fs.open(partFile)
        val reader = new java.io.BufferedReader(new java.io.InputStreamReader(inputStream))
        
        var line: String = reader.readLine()
        while (line != null) {
          writer.write(line)
          writer.newLine()
          line = reader.readLine()
        }
        
        reader.close()
        inputStream.close()
      }
      
      writer.close()
      outputStream.close()
      
      println(s"Successfully merged ${partFiles.size} files into $outputFile")
      
    } catch {
      case e: Exception => 
        println(s"Error merging files: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      fs.close()
    }
  }
}