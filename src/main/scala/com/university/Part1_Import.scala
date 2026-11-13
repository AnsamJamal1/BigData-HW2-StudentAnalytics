package com.university

import org.mongodb.scala._
import org.mongodb.scala.model._
import org.mongodb.scala.result._
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.Date
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Try, Success, Failure}

object Part1_Import {
  def main(args: Array[String]): Unit = {
    println("Starting MongoDB Import...")
    val client = MongoClient("mongodb://localhost:27017")
    val db = client.getDatabase("university")
    val collection = db.getCollection("students")


    Await.result(collection.drop().toFuture(), 10.seconds)
    println("Dropped existing 'students' collection.")

    val lines = Source.fromFile("students.csv").getLines().drop(1)

    val docs: Seq[Document] = lines.flatMap { line =>
      val trimmed = line.trim

      if (trimmed.isEmpty) None
      else {
        Try {
          val parts = trimmed.split(",", 5)
          val id = parts(0).toInt
          val firstName = parts(1)
          val lastName = parts(2)
          val dobStr = parts(3)
          val rest = parts(4)

          val lastComma = rest.lastIndexOf(",")
          val coursesStr = rest.substring(0, lastComma).stripPrefix("[").stripSuffix("]")
          val gpa = rest.substring(lastComma + 1).toDouble

          val courseIds: Seq[Int] =
            if (coursesStr.isEmpty) Seq.empty
            else coursesStr.split(",").map(_.trim.toInt).toSeq

          val dob: Date = Date.from(
            LocalDate.parse(dobStr).atStartOfDay(ZoneOffset.UTC).toInstant
          )

          Document(
            "_id" -> id,
            "firstName" -> firstName,
            "lastName" -> lastName,
            "dateOfBirth" -> dob,
            "courseIds" -> courseIds,
            "gpa" -> gpa
          )
        }.toOption
      }
    }.toSeq


    Await.result(collection.insertMany(docs).toFuture(), 30.seconds)
    println(s"Successfully inserted ${docs.size} documents.")

    Await.result(collection.createIndex(Indexes.ascending("courseIds")).toFuture(), 10.seconds)
    Await.result(collection.createIndex(Indexes.ascending("gpa")).toFuture(), 10.seconds)
    println("Created indexes on courseIds and gpa.")

    val jsonLines = Await.result(collection.find().toFuture(), 30.seconds).map(_.toJson)
    val pw = new java.io.PrintWriter("students.json")
    jsonLines.foreach(pw.println)
    pw.close()
    println("Saved students.json for Spark.")

    val count = Await.result(collection.countDocuments().toFuture(), 10.seconds)
    println(s"Total Document count in MongoDB: $count")

    println("Indexes list:")
    Await.result(collection.listIndexes().toFuture(), 10.seconds).foreach { idx =>
      println(idx.toJson)
    }

    client.close()
    println("Done!")
  }

}

