package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val rectangle_coords = queryRectangle.split(',').map(_.toDouble)
    val point_coords = pointString.split(',').map(_.toDouble)
    // check pointX is between/equal rect x1 and x2
    // check pointY is between/equal rect y1 and y2
    point_coords(0) >= rectangle_coords(0) &&
    point_coords(0) <= rectangle_coords(2) &&
    point_coords(1) >= rectangle_coords(1) &&
    point_coords(1) <= rectangle_coords(3)
  }

  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {
    val point1_coords = pointString1.split(',').map(_.toDouble)
    val point2_coords = pointString2.split(',').map(_.toDouble)
    val distance_between_x = point1_coords(0) - point2_coords(0)
    val distance_between_y = point1_coords(1) - point2_coords(1)
    val distance_between_coords = math.sqrt(math.pow(distance_between_x, 2) + math.pow(distance_between_y, 2))
    // distance between points is less/equal passed in distance
    distance_between_coords <= distance
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", ST_Contains _)

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", ST_Contains _)

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within", ST_Within _)

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within", ST_Within _)
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
