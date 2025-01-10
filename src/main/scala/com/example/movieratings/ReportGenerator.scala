package com.example.movieratings

import scala.io.Source
import scala.collection.mutable
import java.io.File

object ReportGenerator {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println(
        "The command needs 3 parameters: runMain com.example.movieratings.ReportGenerator " +
        "/path/to/movie_titles.txt /path/to/training_set /path/to/output-report.csv"
      )
      sys.exit(1)
    }

    val Array(movieTitlesFile, trainingSetDir, outputReport) = args.map(new File(_))

    val moviesInfo: Map[Int, (Int, String)] = readMovieTitles(movieTitlesFile)

    val ratingsData: Map[Int, (Long, Int)] = readRatings(trainingSetDir)

    val filteredMovies = ratingsData.flatMap { case (movieId, (sumRatings, count)) =>

      moviesInfo.get(movieId).map { case (year, title) =>
        val avgRating = sumRatings.toDouble / count
        (title, year, avgRating, count)
      }
      
    }.filter { case (_, year, _, count) =>
      year >= 1970 && year <= 1990 && count > 1000
    }

    val sortedMovies = filteredMovies.toList.sortBy { case (title, _, avg, _) =>
      (-avg, title)
    }

    val csvRecords = sortedMovies.map { case (title, year, avg, numberReviews) =>
      List(title, year, avg, numberReviews)
    }

    CsvUtils.writeToFile(csvRecords, outputReport)

    println(s"Report generated in the directory: ${outputReport.getAbsolutePath}")
  }

  def readMovieTitles(file: File): Map[Int, (Int, String)] = {
    val records = CsvUtils.readFromFileAsList(file)

    records.flatMap { record =>

      val movieIdString = record.get(0)
      val yearString = record.get(1)
      val title = record.get(2)

      if (yearString != "NULL" && !yearString.isEmpty)
      {
        val movieId = movieIdString.toInt
        val year    = yearString.toInt
        Some(movieId -> (year, title))
      }
      else
      {
        None
      }
    }.toMap
  }

  def readRatings(trainingDir: File): Map[Int, (Long, Int)] = {
    val accumulator = mutable.Map[Int, (Long, Int)]().withDefaultValue((0L, 0))

    for {
      file <- trainingDir.listFiles()
      if file.isFile && file.getName.endsWith(".txt")
    } {
      val records = CsvUtils.readFromFileAsList(file)

      if(records.nonEmpty){
        val movieLine = records.head.get(0).trim
        val movieId = movieLine.stripSuffix(":").toInt

        var (sumRatings, count) = accumulator(movieId)

        records.tail.foreach { record =>
          val rating = record.get(1).toInt
          sumRatings += rating
          count += 1
        }
        accumulator(movieId) = (sumRatings, count)
      }
    }
    accumulator.toMap
  }
}

