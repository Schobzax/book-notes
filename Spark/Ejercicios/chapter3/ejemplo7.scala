package main.scala.chapter3.ejemplo7

import org.apache.spark.sql.Row

object ejemplo {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("Ejemplo6")
            .getOrCreate()
        
        import spark.implicits._

        val row = Row(350, true, "Learning Spark 2E", null)

        row.getInt(0)
        row.getBoolean(1)
        row.getString(2)

        case class DeviceIoTData (battery_level: Long,
                                  c02_level: Long,
                                  cca2: String,
                                  cca3: String,
                                  cn: String,
                                  device_id: Long,
                                  device_name: String,
                                  humidity: Long,
                                  ip: String,
                                  latitude: Double,
                                  lcd: String,
                                  longitude: Double,
                                  scale: String,
                                  temp: Long,
                                  timestamp: Long)

        val ds = spark.read.json("iot_devices.json")
            .as[DeviceIoTData]
        
        ds.show(5, false)
    }
}