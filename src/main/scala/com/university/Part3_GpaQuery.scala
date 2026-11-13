package com.university

import org.mongodb.scala._
import org.mongodb.scala.model._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Sorts._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Try, Success, Failure}

object Part3_GpaQuery {
  def main(args: Array[String]): Unit = {
    val client = MongoClient("mongodb://localhost:27017")
    val collection = client.getDatabase("university").getCollection("students")

    var minGpa: Option[Double] = None
    while (minGpa.isEmpty) {
      print("Enter minimum GPA (0.0 - 4.0): ")
      val input = StdIn.readLine().trim
      Try(input.toDouble) match {
        case Success(v) if v >= 0.0 && v <= 4.0 => minGpa = Some(v)
        case _ => println("Invalid. Enter a number between 0.0 and 4.0")
      }

    }

    val gpa = minGpa.get

    val results = Await.result(
      collection.find(gt("gpa", gpa))
        .sort(descending("gpa"))
        .toFuture(),
      15.seconds
    )

    println(s"\nStudents with GPA > $gpa:")

    results.foreach { doc =>
      println(f"${doc.getInteger("_id")}%-5d ${doc.getString("firstName")} ${doc.getString("lastName")}%-20s GPA: ${doc.getDouble("gpa")}%.2f")
    }
    println(f"Total: ${results.size}")

    println("\n--- Combined query (Optional) ---")
    print("Enter course ID (or press Enter to skip): ")
    val courseInput = StdIn.readLine().trim

    print("Enter minimum GPA (or press Enter to skip): ")
    val gpaInput = StdIn.readLine().trim


    var combinedFilter = List.empty[org.bson.conversions.Bson]
    if (courseInput.nonEmpty) {
      Try(courseInput.toInt).foreach(cid => combinedFilter = combinedFilter :+ equal("courseIds", cid))
    }
    if (gpaInput.nonEmpty) {
      Try(gpaInput.toDouble).foreach(g => combinedFilter = combinedFilter :+ gt("gpa", g))
    }



    if (combinedFilter.nonEmpty) {
      val combinedResults = Await.result(
        collection.find(and(combinedFilter: _*)).sort(descending("gpa")).toFuture(),
        15.seconds
      )
      println(s"\nCombined results (${combinedResults.size} matches):")
      combinedResults.foreach { doc =>
        println(f"${doc.getInteger("_id")} - ${doc.getString("firstName")} ${doc.getString("lastName")} - GPA: ${doc.getDouble("gpa")}%.2f - Courses: ${doc.getList("courseIds", classOf[Integer])}")
      }

    }

    client.close()
  }
}


