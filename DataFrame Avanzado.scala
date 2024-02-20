// Databricks notebook source
// Importar las bibliotecas necesarias
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Crear una sesión de Spark
val spark = SparkSession.builder.appName("Ejercicios 1").getOrCreate()
val options = Map("inferSchema"->"true","delimiter"->",","header"->"true")
// Leer el archivo
val dfGPS = spark.read.options(options).csv("dbfs:/FileStore/tables/googleplaystore.csv")
// Sin esa App
val dfGPSFiltro = dfGPS.filter("App != 'Life Made WI-FI Touchscreen Photo Frame'").show()

// COMMAND ----------

// Importar las bibliotecas necesarias
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Crear una sesión de Spark
val spark = SparkSession.builder.appName("Ejercicios 1").getOrCreate()
val options = Map("inferSchema"->"true","delimiter"->",","header"->"true")
// Leer el archivo
val dfGPS = spark.read.options(options).csv("dbfs:/FileStore/tables/googleplaystore.csv")
// Sustituir valores
val dfGPSRatingNotNaN = dfGPS.withColumn("Rating", when(col("Rating")==="NaN", "").otherwise(col("Rating"))).filter(col("Rating") === "").show()

// COMMAND ----------

// Importar las bibliotecas necesarias
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Crear una sesión de Spark
val spark = SparkSession.builder.appName("Ejercicios 1").getOrCreate()
val options = Map("inferSchema"->"true","delimiter"->",","header"->"true")
// Leer el archivo
val dfGPS = spark.read.options(options).csv("dbfs:/FileStore/tables/googleplaystore.csv")
// Sustituir valores
val dfGPSRatingNotNaN = dfGPS.withColumn("Type", when(col("Type")==="NaN", "Unknown").otherwise(col("Type"))).filter(col("Type") === "Unknown").show()

// COMMAND ----------

// Importar las bibliotecas necesarias
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Crear una sesión de Spark
val spark = SparkSession.builder.appName("Ejercicios 1").getOrCreate()
val options = Map("inferSchema"->"true","delimiter"->",","header"->"true")
// Leer el archivo
val dfGPS = spark.read.options(options).csv("dbfs:/FileStore/tables/googleplaystore.csv")
// Caracteristicas Varian Según Dispositivo
val dfGPSReplace = dfGPS.withColumn("Varia", when((col("Current Ver") === "Varies with device") || (col("Android Ver") === "Varies with device"), "True").otherwise("False")).show()

// COMMAND ----------

// Importar las bibliotecas necesarias
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Crear una sesión de Spark
val spark = SparkSession.builder.appName("Ejercicios 1").getOrCreate()
val options = Map("inferSchema"->"true","delimiter"->",","header"->"true")
// Leer el archivo
val dfGPS = spark.read.options(options).csv("dbfs:/FileStore/tables/googleplaystore.csv")
// Switch
// val dfGPSSwitch = dfGPS.withColumn("Installs", regexp_replace(col("Installs"), "[+,]", "").cast("bigint"))
val dfGPSSwitch = dfGPS.withColumn("Installs", regexp_replace(split(col("Installs"),"\\+",0)(0), "[^0-9]",""))
val dfGPSSwitchResult = dfGPSSwitch.withColumn("Frec_Download", 
  when(col("Installs") < 50000, "Baja")
  .when(col("Installs") >= 50000 && col("Installs") < 1000000, "Media")
  .when(col("Installs") >= 1000000 && col("Installs") < 50000000, "Alta")
  .when(col("Installs") >= 50000000, "Muy Alta")
  .otherwise("ERROR")
)
dfGPSSwitchResult.show()
// 6
// A
val a=dfGPSSwitchResult.filter(col("Frec_Download")==="Muy Alta" && col("Rating")>4.5).show()
// B
val b=dfGPSSwitchResult.filter(col("Frec_Download")==="Muy Alta" && col("Price")===0).count()
// C
val c=dfGPSSwitchResult.filter(col("Price")<13.0).show()

// COMMAND ----------

// Importar las bibliotecas necesarias
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Crear una sesión de Spark
val spark = SparkSession.builder.appName("Ejercicios 1").getOrCreate()
val options = Map("inferSchema"->"true","delimiter"->",","header"->"true")
// Leer el archivo
val dfGPS = spark.read.options(options).csv("dbfs:/FileStore/tables/googleplaystore.csv")
val dfSample = dfGPS.sample(withReplacement = false, fraction = 0.1, seed = 123).show()
