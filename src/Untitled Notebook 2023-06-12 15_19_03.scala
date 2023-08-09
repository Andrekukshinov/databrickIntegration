// Databricks notebook source
import ch.hsr.geohash.GeoHash
import com.byteowls.jopencage.JOpenCageGeocoder
import com.byteowls.jopencage.model.{JOpenCageForwardRequest, JOpenCageLatLng, JOpenCageResponse}
import org.apache.spark.sql.functions.{lit, udf, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

// COMMAND ----------

 val percussion: Int = 11

// COMMAND ----------

    spark.conf.set("fs.azure.account.auth.type.stkuksh0tf0infwesteurope.dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type.stkuksh0tf0infwesteurope.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id.stkuksh0tf0infwesteurope.dfs.core.windows.net", "f286a4cd-8fcf-4064-9120-9738ad36ea4e")
    spark.conf.set("fs.azure.account.oauth2.client.secret.stkuksh0tf0infwesteurope.dfs.core.windows.net", "_ei8Q~H-_hcc0vnrhRns4~8-2HFfWHCKvb2cfbVN")
    spark.conf.set("fs.azure.account.oauth2.client.endpoint.stkuksh0tf0infwesteurope.dfs.core.windows.net", "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")

// COMMAND ----------

    val hotelsScheme = StructType(Seq())
      .add(StructField("id", LongType, true, Metadata.empty))
      .add(StructField("name", StringType, true, Metadata.empty))
      .add(StructField("country", StringType, true, Metadata.empty))
      .add(StructField("city", StringType, true, Metadata.empty))
      .add(StructField("address", StringType, true, Metadata.empty))
      .add(StructField("latitude", DoubleType, true, Metadata.empty))
      .add(StructField("longitude", DoubleType, true, Metadata.empty))
      .add(StructField("geohash", StringType, true, Metadata.empty))

// COMMAND ----------

    val fileZipped = spark.read.schema(hotelsScheme).option("header", true).csv("abfss://data@stkuksh0tf0infwesteurope.dfs.core.windows.net/src/m06sparkbasics/hotels/")


// COMMAND ----------

    var testParqet = spark.read.parquet("abfss://data@stkuksh0tf0infwesteurope.dfs.core.windows.net/src/m06sparkbasics/weather/")

    testParqet = testParqet.withColumn("geohash", lit("0"))

// COMMAND ----------

testParqet.count

// COMMAND ----------

case class Hotel(id: Option[Long], name: String, country: String, city: String, address: String, var latitude: Option[Double], var longitude: Option[Double], var geohash: String)

// COMMAND ----------

case class Weather(lng: Double, lat: Double, avg_tmpr_f: Double, avg_tmpr_c: Double, wthr_date: String, var geohash: String)

// COMMAND ----------

val weather = testParqet.as[Weather]

// COMMAND ----------

val hotelsDF = fileZipped.as[Hotel]

// COMMAND ----------

    def processCpyCoordinates(country: String, city: String):Double = {
      val stringBuilder: StringBuilder = new StringBuilder()
        .append(country)
        .append(", ")
        .append(city)
      val jOpenCageGeocoder: JOpenCageGeocoder = new JOpenCageGeocoder("68afdec778e04797afe980badeded299");
      val request: JOpenCageForwardRequest = new JOpenCageForwardRequest(stringBuilder.toString);
      val response: JOpenCageResponse = jOpenCageGeocoder.forward(request)
      response.getFirstPosition.getLat
    }


// COMMAND ----------

    def processCoordinates(hotel: Hotel): JOpenCageLatLng = {
      val stringBuilder: StringBuilder = new StringBuilder()
        .append(hotel.country)
        .append(", ")
        .append(hotel.city)
      val jOpenCageGeocoder: JOpenCageGeocoder = new JOpenCageGeocoder("68afdec778e04797afe980badeded299");
      val request: JOpenCageForwardRequest = new JOpenCageForwardRequest(stringBuilder.toString);
      val response: JOpenCageResponse = jOpenCageGeocoder.forward(request)
      response.getFirstPosition
    }


// COMMAND ----------

    val geohashedHotels = hotelsDF.map(hotel => {
      val latitude: Option[Double] = hotel.latitude
      val longitude: Option[Double] = hotel.longitude
      var hash: String = "nwlly"
      if (latitude.isEmpty || longitude.isEmpty) {
        val coords: JOpenCageLatLng = processCoordinates(hotel)
        hotel.latitude = Option[Double](coords.getLat)
        hotel.longitude = Option[Double](coords.getLng)
      }
      hash = GeoHash.withCharacterPrecision(hotel.latitude.get, hotel.longitude.get, percussion).toBase32.substring(0, 4)
      hotel.geohash = hash
      hotel
    }).toDF


// COMMAND ----------

    val geohashedWeathers = weather.map(wh => {
      val latitude = wh.lat
      val longitude = wh.lng
      val hash = GeoHash.withCharacterPrecision(latitude, longitude, percussion).toBase32.substring(0, 4)
      wh.geohash = hash
      wh
    }).toDF

// COMMAND ----------

    val result = geohashedWeathers.join(geohashedHotels, Seq("geohash"), "left")


// COMMAND ----------

result.write.format("parquet").save("abfss://data@stkuksh0tf0infwesteurope.dfs.core.windows.net/out/result")

// COMMAND ----------

// %fs ls abfss://homeworks@source0for0homeworks.dfs.core.windows.net/firstHW/out/


// COMMAND ----------

// %sql
// select * from parquet.`abfss://homeworks@source0for0homeworks.dfs.core.windows.net/hotels/m06sparkbasics/weather/year=2016/month=10/day=01/.part-00229-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet.crc`
