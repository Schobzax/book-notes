package main.scala.chapter3.ejemplo8

import org.apache.spark.sql.Row

object ejemplo {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("Ejemplo6")
            .getOrCreate()
        
        import spark.implicits._

        // Volvemos a crear el schema del ejercicio anterior
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

        // Y el dataset también
        val ds = spark.read.json("iot_devices.json")
            .as[DeviceIoTData]

        // 1. Filtro

        val filterTempDS = ds.filter({d => {d.temp > 30 && d.humidity > 70}})

        filterTempDS.show(5, false)

        case class DeviceTempByCountry(temp: Long,
                                       device_name: String,
                                       device_id: Long,
                                       cca3: String)
        
        val dsTemp = ds.filter(d => {d.temp > 25})
                       .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
                       .toDF("temp", "device_name", "device_id", "cca3")
                       .as[DeviceTempByCountry]
                    
        dsTemp.show(5, false)

        val device = dsTemp.first()
        println(device)

        // Otra manera de escribir lo de arriba.
        val dsTemp2 = ds
            .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
            .filter("temp > 25")
            .as[DeviceTempByCountry]

        // Detect failing devices with battery levels below a threshold.
        // Detectar dispositivos con baterías por debajo de un umbral.
        ds.filter($"battery_level" < 8).show(5, false)

        // Identify offending countries with high levels of CO2 emissions.
        // Identificar países con niveles de CO2 altos.
        ds.groupBy("cn")
            .agg(avg("c02_level").alias("AvgCO2"))
            .sort(desc("AvgCO2"))

        // Weirdly it's not who you'd think: Gabon is the highest, followed by Falkland Islands, Monaco, Kosovo, San Marino.

        // Compute the min and max values for temperature, battery level, CO2, and humidity.
        // Mínimo y máximo de temperatura, batería, CO2 y humedad.
        ds.select(max("temp"), min("temp"), max("battery_level"), min("battery_level"), max("c02_level"), min("c02_level"), max("humidity"), min("humidity")).show(5, false)

        // Sort and group by average temperature, CO2, humidity, and country.
        // Ordenar y agrupar por tempreatura media, CO2, humedad y país.
        ds.select("temp","humidity","cn")
            .groupBy($"cn")
            .avg()
            .sort($"avg(temp)".desc, $"avg(humidity)".desc).show(10, false)

    }
}