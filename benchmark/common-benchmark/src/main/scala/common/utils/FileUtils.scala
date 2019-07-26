package common.utils

import java.io.FileInputStream

import org.apache.commons.io.{FileUtils => FileUtilsCommonsIO}

import scala.io.Source
import java.io.File

import common.benchmark.input.Parsers

object FileUtils {
  def getRecursiveListOfFiles(dir: File): Array[File] = {
    val these = dir.listFiles
    these ++ these.filter(_.isDirectory).flatMap(getRecursiveListOfFiles)
  }

  def orderAndReadFile(path: String): Iterator[String] = {
    def comparator(first: String, second: String) =
      Parsers.extractTimestamp(first) < Parsers.extractTimestamp(second)

    //list files in directory
    val listFiles = getRecursiveListOfFiles(new File(path)).filter(f => !f.getPath.endsWith(".crc") & !f.getPath.contains("SUCCESS") & !f.getPath.contains("sorted") & !f.isDirectory)
    listFiles.foreach(file => println(s"File to be included: ${file.getPath}"))
    //read in all files
    var file: List[String] = List()
    listFiles.toList.foreach { f =>
      val resource = new FileInputStream(f.getPath)
      file = file.++(Source.fromInputStream(resource).getLines)
    }

    //make sure the iterator is ordered by timestamp
    val fileOrdered = file.sortWith(comparator).toIterator

    fileOrdered
  }

  def cleanDirectories(): Unit = {
    val dir: File = new File("/tmp/")
    dir.listFiles().foreach { f =>
      if (f.getName.startsWith("kafka") ||
        f.getName.startsWith("flink") ||
        f.getName.startsWith("spark") ||
        f.getName.startsWith("storm") ||
        f.getName.startsWith("blobStore") ||
        f.getName.startsWith("blockmgr")) {
        println(f.getAbsolutePath)
        FileUtilsCommonsIO.deleteDirectory(f)
      }
    }
  }
}
