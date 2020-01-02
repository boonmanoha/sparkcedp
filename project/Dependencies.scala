// Dependencies file for maintaining library dependencies
import sbt._

object Dependencies {

  object versions {
    lazy val spark = "2.4.3"
    lazy val dpsUtils = "0.1.1"
    lazy val stocator = "1.0.29-IBM-SDK"
    lazy val hadoopHdfs = "2.7.3"
    lazy val awsJavaSdk = "1.7.4"
    lazy val hadoopAws = "2.7.3"
    lazy val mongoDb = "2.1.0"
  }

  /* Spark */
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark //% "provided"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % versions.spark //% "provided"

  /* IAM Authentication */
  lazy val iamAuthentication = "com.ibm.dps" % "dps-utils_2.11" % versions.dpsUtils
  lazy val stocator = "com.ibm.stocator" % "stocator" % versions.stocator

  /* AWS Libraries */
  lazy val hadoopHdfs = "org.apache.hadoop" % "hadoop-hdfs" % versions.hadoopHdfs
  lazy val awsJavaSdk = "com.amazonaws" % "aws-java-sdk" % versions.awsJavaSdk
  lazy val hadoopAws = "org.apache.hadoop" % "hadoop-aws" % versions.hadoopAws

  /* Mongo Libraries */
  lazy val mongoDb = "org.mongodb.spark" %% "mongo-spark-connector" % versions.mongoDb

  lazy val rootDependencies = Seq(
    sparkCore,
    sparkSql,
//    stocator,
//    iamAuthentication
    hadoopHdfs,
    hadoopAws,
    awsJavaSdk,
    mongoDb
  )
}