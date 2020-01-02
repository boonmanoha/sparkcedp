import org.apache.spark.sql.SparkSession
import java.util.Properties
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb._


object ScalaJDBCTest {
  println("Inside ScalaJDBCTest - DUMMY")

   def main(args: Array[String]) {  
   val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.master", "local")
      .config("spark.mongodb.input.uri", "mongodb://ibm_cloud_a9be0a7b_38da_44b1_bcad_da2fe17a411b:2ffba7d767ed64fe4e1ee1d5729fcceb06162796f6d0b540c6f1bc08ca569a2e@413deb38-a3eb-48b9-86a0-5258eeb9854e-0.2adb0220806343e3ae11df79c89b377f.databases.appdomain.cloud:30006,413deb38-a3eb-48b9-86a0-5258eeb9854e-1.2adb0220806343e3ae11df79c89b377f.databases.appdomain.cloud:30006/ibmclouddb?authSource=admin&replicaSet=replset&ssl=true")
      .config("spark.mongodb.output.uri", "mongodb://ibm_cloud_a9be0a7b_38da_44b1_bcad_da2fe17a411b:2ffba7d767ed64fe4e1ee1d5729fcceb06162796f6d0b540c6f1bc08ca569a2e@413deb38-a3eb-48b9-86a0-5258eeb9854e-0.2adb0220806343e3ae11df79c89b377f.databases.appdomain.cloud:30006,413deb38-a3eb-48b9-86a0-5258eeb9854e-1.2adb0220806343e3ae11df79c89b377f.databases.appdomain.cloud:30006/ibmclouddb?authSource=admin&replicaSet=replset&ssl=true")
      .config("javax.net.ssl.trustStore", "/opt/mongo-dst.pem")
      .getOrCreate()

   spark.sparkContext.setLogLevel("DEBUG")

   println("In main calling JDBC function")  
   runMongoDBExample(spark)
   println("Finished making JDBC call") 
 
   spark.stop() 
  }
  private def runJdbcDatasetExample(spark: SparkSession): Unit = {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    println("In runJdbcDatasetExample")
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "com.ibm.db2.jcc.DB2Driver")
      .option("url", "jdbc:db2://bigsql.data.zc2.ibm.com:52000/BIGSQL:sslConnection=true;sslTrustStoreLocation=/opt/ibm-truststore.jks;sslTrustStorePassword=vLYpWnJoi27ir7ZM;")
      .option("dbtable", "mpw_v2intr2.TEST1")
      .option("user", "siadmin_us_ibm_com")
      .option("password", "Dstsie123")
      .load()
      
      println("Getting User Profile Data")  
      jdbcDF.printSchema() 
      val rowcount = jdbcDF.count() 
      println("JDBC load complated " + rowcount + " rows loaded" )
  }

  private def runMongoDBExample(spark: SparkSession): Unit = {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    println("In run MONGO Dataset Example")
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "mongodb.jdbc.MongoDriver")
      .option("url", "jdbc:mongodb://ibm_cloud_a9be0a7b_38da_44b1_bcad_da2fe17a411b:2ffba7d767ed64fe4e1ee1d5729fcceb06162796f6d0b540c6f1bc08ca569a2e@413deb38-a3eb-48b9-86a0-5258eeb9854e-0.2adb0220806343e3ae11df79c89b377f.databases.appdomain.cloud:30006,413deb38-a3eb-48b9-86a0-5258eeb9854e-1.2adb0220806343e3ae11df79c89b377f.databases.appdomain.cloud:30006/ibmclouddb?authSource=admin&replicaSet=replset&ssl=true&sslRootCert=/opt/mongo-dst.pem")
      .option("dbtable", "athena_dev.users")
      .option("user", "ibm_cloud_a9be0a7b_38da_44b1_bcad_da2fe17a411b")
      .option("password", "2ffba7d767ed64fe4e1ee1d5729fcceb06162796f6d0b540c6f1bc08ca569a2e")
      .load()

  }

  private def runMongoDBDatasetExample(spark: SparkSession): Unit = {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    println("In run MONGO Dataset Example")
    val readConfig = ReadConfig(Map("uri" -> "mongodb://ibm_cloud_a9be0a7b_38da_44b1_bcad_da2fe17a411b:2ffba7d767ed64fe4e1ee1d5729fcceb06162796f6d0b540c6f1bc08ca569a2e@413deb38-a3eb-48b9-86a0-5258eeb9854e-0.2adb0220806343e3ae11df79c89b377f.databases.appdomain.cloud:30006,413deb38-a3eb-48b9-86a0-5258eeb9854e-1.2adb0220806343e3ae11df79c89b377f.databases.appdomain.cloud:30006/ibmclouddb?authSource=admin&replicaSet=replset&ssl=true",
                                "database" -> "athena_dev", "collection" -> "users"))
    val zipDf = MongoSpark.load(spark,readConfig)
      println("Getting User Profile Data")  
      zipDf.printSchema() 
      val rowcount = zipDf.count() 
      println("JDBC load complated " + rowcount + " rows loaded." )
  } 
  
//  private def runJsonDatasetExample(spark: SparkSession): Unit = {
//    // $example on:json_dataset$
//    // Primitive types (Int, String, etc) and Product types (case classes) encoders are
//    // supported by importing this when creating a Dataset.
//    import spark.implicits._
//
//    // A JSON dataset is pointed to by path.
//    // The path can be either a single text file or a directory storing text files
//    val path = "people.json"
//    val peopleDF = spark.read.json(path)
//
//    // The inferred schema can be visualized using the printSchema() method
//    peopleDF.printSchema()
//    // root
//    //  |-- age: long (nullable = true)
//    //  |-- name: string (nullable = true)
//
//    // Creates a temporary view using the DataFrame
//    peopleDF.createOrReplaceTempView("people")
//
//    // SQL statements can be run by using the sql methods provided by spark
//    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
//    teenagerNamesDF.show()
//    // +------+
//    // |  name|
//    // +------+
//    // |Justin|
//    // +------+
//
//    // Alternatively, a DataFrame can be created for a JSON dataset represented by
//    // a Dataset[String] storing one JSON object per string
//    val otherPeopleDataset = spark.createDataset(
//      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
//    val otherPeople = spark.read.json(otherPeopleDataset)
//    otherPeople.show()
//    // +---------------+----+
//    // |        address|name|
//    // +---------------+----+
//    // |[Columbus,Ohio]| Yin|
//    // +---------------+----+
//    // $example off:json_dataset$
//  }
}