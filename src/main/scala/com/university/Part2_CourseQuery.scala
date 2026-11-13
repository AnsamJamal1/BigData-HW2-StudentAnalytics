package com.university

import org.mongodb.scala._
import org.mongodb.scala.model._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Projections._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.Try

object Part2_CourseQuery {
  def main(args: Array[String]): Unit = {
    val client = MongoClient("mongodb://localhost:27017")
    val db = client.getDatabase("university")
    val collection = db.getCollection("students")

    var courseId: Option[Int] = None
    while (courseId.isEmpty) {
      print("Enter course ID (integer): ")
      val input = StdIn.readLine().trim
      courseId = Try(input.toInt).toOption
      if (courseId.isEmpty) println("Invalid input. Please enter an integer.")
    }

    val cid = courseId.get


    val filter = equal("courseIds", cid)

    val explainDoc = Await.result(
      collection.find(filter).explain(com.mongodb.ExplainVerbosity.EXECUTION_STATS).toFuture(),
      15.seconds
    )

    val explainJson = explainDoc.toJson
    val usedIndex = explainJson.contains("IXSCAN")
    println(s"\nIndex used (IXSCAN found): $usedIndex")

    val results = Await.result(
      collection.find(filter)
        .projection(include("_id", "firstName", "lastName", "gpa"))
        .sort(ascending("lastName"))
        .toFuture(),
      15.seconds
    )


    println(s"\nStudents enrolled in course $cid:")
    println(f"${"ID"}%-5s ${"Full Name"}%-25s ${"GPA"}%-5s")
    println("-" * 40)
    results.foreach { doc =>
      val id = doc.getInteger("_id")
      val first = doc.getString("firstName")
      val last = doc.getString("lastName")
      val gpa = doc.getDouble("gpa")
      println(f"$id%-5d $first $last%-20s $gpa%-5.2f")
    }
    println(f"\nTotal matches: ${results.size}")

    client.close()
  }

}



