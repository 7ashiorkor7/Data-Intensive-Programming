package assignment22

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{ClusteringEvaluator}
import org.apache.spark.sql.functions.{min, max}



class Assignment {

  val spark: SparkSession = SparkSession.builder()
    .appName("Assignment")
    .config("spark.driver.host", "localhost")
    .master("local")
    .getOrCreate()

  // the data frame to be used in tasks 1 and 4
  val dataD2: DataFrame = spark.read
    .format("csv")
    .option("delimiter", ",")  // optional, since the default delimiter is comma
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/dataD2.csv")

  // the data frame to be used in task 2
  val dataD3: DataFrame = spark.read
    .format("csv")
    .option("delimiter", ",")  // optional, since the default delimiter is comma
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/dataD3.csv")


  // the data frame to be used in task 3 (based on dataD2 but containing numeric labels)
  val dataD2WithLabels: DataFrame = dataD2.selectExpr(
    "a",
    "b",
    "case when LABEL = 'Ok' then 1 else 0 end AS labelasint")


  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
    // Handle null values.
    val cleanDf = df.na.drop()

    // Execute kmeans algorithm.
    val kmModel = execKmeans(cleanDf, k, Array("a", "b"))

    // Get min and max values for each parameter.
    val paramA = getMaxAndMinValues("a", cleanDf)
    val paramB = getMaxAndMinValues("b", cleanDf)

    // return cluster centers.
    return kmModel.clusterCenters.map( item => (item(0)*(paramA._2-paramA._1)+paramA._1, item(1)*(paramB._2-paramB._1)+paramB._1))
  }

  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {
    // Execute kmeans algorithm.
    val kmModel = execKmeans( df, k, Array("a", "b", "c"))

    // Get min and max values for each parameter.
    val paramA = getMaxAndMinValues("a", df)
    val paramB = getMaxAndMinValues("b", df)
    val paramC = getMaxAndMinValues("c", df)


    // return cluster centers.
    return kmModel.clusterCenters.map( item => (item(0)*(paramA._2-paramA._1)+paramA._1, item(1)*(paramB._2-paramB._1)+paramB._1, item(2)*(paramC._2-paramC._1)+paramC._1))
  }

  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    // Execute kmeans algorithm.
    val kmModel = execKmeans(df, k, Array( "a", "b", "labelasint"))

    // sort results.
    var centers = kmModel.clusterCenters.map( item => (item(0), item(1), item(2))).sortWith(_._3 < _._3)

    // Get min and max values for each parameter.
    val paramA = getMaxAndMinValues("a", df)
    val paramB = getMaxAndMinValues("b", df)

    // Return cluster centers.
    return centers.slice(0, 2).map( item => (item._1*(paramA._2-paramA._1)+paramA._1, item._2*(paramB._2-paramB._1)+paramB._1))
  }

  // Parameter low is the lowest k and high is the highest one.
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
    // Result array.
    val result: Array[(Int, Double)] = new Array[(Int, Double)](high-low+1)

    // Calculate silhouette score for each round.
    val index = 0
    for(k <- low to high)
    {
      // push result.
      result.update(k-low, execSilhouette(df, k,Array("a", "b")))
    }

    // return result.
    return result
  }

  // Executes kmeans function.
  def execKmeans(df: DataFrame, k: Int, parameterArray: Array[String]): KMeansModel = {
    // Prepare vector.
    val vectorAssembler = new VectorAssembler()
      .setInputCols(parameterArray)
      .setOutputCol("featuresVector")

    // Create pipeline.
    val scaledDf = new MinMaxScaler().setInputCol("featuresVector").setOutputCol("features")
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler, scaledDf))
    val pipeLine = transformationPipeline.fit(df)
    val transformedData = pipeLine.transform(df)

    // Prepare for kmeans algorithm.
    val kmeans = new KMeans().setK(k).setSeed(1L)

    // Fit data.
    val kmModel = kmeans.fit(transformedData)

    // Return data.
    return kmModel
  }

  def execSilhouette(df: DataFrame, k: Int, parameterArray: Array[String]): (Int, Double) = {
    // Prepare vector.
    val vectorAssembler = new VectorAssembler()
      .setInputCols(parameterArray)
      .setOutputCol("featuresVector")

    // Create pipeline.
    val scaledDf = new MinMaxScaler().setInputCol("featuresVector").setOutputCol("features")
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler, scaledDf))
    val pipeLine = transformationPipeline.fit(df)
    val transformedData = pipeLine.transform(df)

    // Prepare for kmeans algorithm.
    val kmeans = new KMeans().setK(k).setSeed(1L)

    // Fit data.
    val kmModel = kmeans.fit(transformedData)

    // Make predictions.
    val predictions = kmModel.transform(transformedData)

    // Evaluate clustering by computing Silhouette score.
    val evaluator = new ClusteringEvaluator()

    // Get score.
    val silhouetteScore = evaluator.evaluate(predictions)

    // Return result.
    return (k, silhouetteScore)
  }

  def getMaxAndMinValues(parameter: String, df: DataFrame): (Double, Double) = {
    // Get values.
    val max = df.selectExpr("MAX(" + parameter + ")").take(1)(0).getDouble(0)
    val min = df.selectExpr("MIN(" + parameter + ")").take(1)(0).getDouble(0)

    // Return result.
    return (min, max)
  }

  //additional task 3 - dirty data
  val dataD2_dirty : DataFrame = spark.read
    .format("csv")
    .option("delimiter", ",")  // optional, since the default delimiter is comma
    .option("header", "true")
    .schema("a DOUBLE, b DOUBLE, LABEL String")
    .option("mode", "DROPMALFORMED")
    .csv("data/dataD2_dirty.csv")
}
