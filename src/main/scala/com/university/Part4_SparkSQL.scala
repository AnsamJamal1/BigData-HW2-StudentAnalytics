package com.university

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Part4_SparkSQL {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StudentAnalytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("=== Loading Data from students.json ===")

    val rawDf = spark.read.json("students.json")
    val df = rawDf.withColumn("dateOfBirth", to_date(col("dateOfBirth.$date")))
    df.printSchema()

    df.createOrReplaceTempView("students")

    // 4.1 Reproducing the MongoDB queries
    println("\n=== 4.1.1 Students in course 103 ===")
    spark.sql("""
      SELECT _id, firstName, lastName, gpa
      FROM students
      WHERE array_contains(courseIds, 103)
      ORDER BY lastName
    """).show(100, truncate = false)

    println("\n=== 4.1.2 Students with GPA > 3.5 ===")
    spark.sql("""
      SELECT _id, firstName, lastName, gpa
      FROM students
      WHERE gpa > 3.5
      ORDER BY gpa DESC
    """).show(100, truncate = false)



    // 4.2 Working with the array column
    println("\n=== 4.2.1 Creating enrollments view ===")
    spark.sql("""
      CREATE OR REPLACE TEMPORARY VIEW enrollments AS
      SELECT _id, firstName, lastName, gpa, explode(courseIds) AS courseId
      FROM students
    """)


    println("\n=== 4.2.2 Per-course statistics ===")
    spark.sql("""
      SELECT
        courseId,
        COUNT(*) AS num_students,
        ROUND(AVG(gpa), 2) AS avg_gpa,
        MIN(gpa) AS min_gpa,
        MAX(gpa) AS max_gpa
      FROM enrollments
      GROUP BY courseId
      ORDER BY num_students DESC
    """).show()

    println("\n=== 4.2.3 Students by number of courses ===")
    spark.sql("""
      SELECT
        size(courseIds) AS num_courses,
        COUNT(*) AS num_students
      FROM students
      GROUP BY size(courseIds)
      ORDER BY num_courses
    """).show()




    // 4.3 Aggregations and window functions

    println("\n=== 4.3.1 Stats by birth year ===")
    spark.sql("""
      SELECT
        year(dateOfBirth) AS birth_year,
        COUNT(*) AS num_students,
        ROUND(AVG(gpa), 2) AS avg_gpa
      FROM students
      GROUP BY year(dateOfBirth)
      ORDER BY birth_year
    """).show()


    println("\n=== 4.3.2 Top 3 students per course ===")
    spark.sql("""
      WITH ranked AS (
        SELECT
          courseId,
          _id,
          firstName,
          lastName,
          gpa,
          DENSE_RANK() OVER (PARTITION BY courseId ORDER BY gpa DESC) AS rank
        FROM enrollments
      )
      SELECT courseId, _id, firstName, lastName, gpa, rank
      FROM ranked
      WHERE rank <= 3
      ORDER BY courseId, rank
    """).show(100, truncate = false)


    println("\n=== 4.3.3 Students with same first & last name ===")
    spark.sql("""
      SELECT
        s1._id AS id1,
        s1.firstName,
        s1.lastName,
        s1.dateOfBirth AS dob1,
        s2._id AS id2,
        s2.dateOfBirth AS dob2
      FROM students s1
      JOIN students s2
        ON s1.firstName = s2.firstName
        AND s1.lastName = s2.lastName
        AND s1._id < s2._id
      ORDER BY s1.lastName, s1.firstName
    """).show(50, truncate = false)


    println("\n=== 4.3.4 Students above average in ALL their courses ===")
    spark.sql("""
      WITH course_avgs AS (
        SELECT courseId, AVG(gpa) AS avg_course_gpa
        FROM enrollments
        GROUP BY courseId
      ),
      student_check AS (
        SELECT
          e._id,
          e.firstName,
          e.lastName,
          e.gpa,
          e.courseId,
          CASE WHEN e.gpa > ca.avg_course_gpa THEN 1 ELSE 0 END AS above_avg
        FROM enrollments e
        JOIN course_avgs ca ON e.courseId = ca.courseId
      )
      SELECT _id, firstName, lastName, gpa
      FROM student_check
      GROUP BY _id, firstName, lastName, gpa
      HAVING MIN(above_avg) = 1
      ORDER BY gpa DESC
""").show(100, truncate = false)




    // 4.4 Writing results back
    println("\n=== 4.4 Writing course_stats to Parquet (since we used Option B) ===")
    val courseStats = spark.sql("""
      SELECT
        courseId,
        COUNT(*) AS num_students,
        ROUND(AVG(gpa), 2) AS avg_gpa,
        MIN(gpa) AS min_gpa,
        MAX(gpa) AS max_gpa
      FROM enrollments
      GROUP BY courseId
      ORDER BY num_students DESC
    """)

    courseStats.write.mode("overwrite").parquet("course_stats.parquet")
    println("Saved to course_stats.parquet")

    println("\n=== Verification: course_stats read back ===")
    val verify = spark.read.parquet("course_stats.parquet")
    verify.show()


    spark.stop()
    println("\nDone! All Spark queries executed successfully.")
  }

}




