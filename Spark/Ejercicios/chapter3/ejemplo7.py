import sys

from pyspark.sql import Row

if __name__ == "__main__":
    spark = (SparkSession
        .builder
        .appName("Ejemplo6")
        .getOrCreate())

    row = Row(350, True, "Learning Spark 2E", None)

    print(row[0])
    print(row[1])
    print(row[2])

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