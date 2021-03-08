# NoisyCsvVisualizer

This is readme is not final and will *definitly* be improved in the future.

A kotlin application for visualizing poor qualitity csv data into useable plots. 

Based on a questionalbe homebrew actor system based on kotlin channels, [Apache Commons CSV](https://commons.apache.org/proper/commons-csv/) for CSV parsing, [Arrow](https://github.com/arrow-kt/arrow) for data transformation and [XChart](https://github.com/knowm/XChart) for data visualization.



## Input(Sample)
|adc_file_name           |adc_ch|frequency_range|begin_datetime_utc           |end_datetime_utc             |duration_s       |begin_i|end_i|f_signal          |f_signal_diffs_time_mean|time_accuracy      |sample_f|sig_abs_power     |umweltspaeher_id|utc_tim |utc_date  |temp_in  |humidity_in|load_cell0|load_cell1|load_cell2|gps_sync_time|gps_lat      |gps_lon       |gps__|gps_|gps_altitude|temp_out |hum_out  |temp_cpu|stm32_uuid               |load_cell0_kg|load_cell1_kg|load_cell2_kg|weight_all_kg|fio_temperature|fio_humidity|fio_pressure|fio_dewPoint|fio_windSpeed|fio_windBearing|fio_cloudCover|fio_uvIndex|fio_visibility|
|------------------------|------|---------------|-----------------------------|-----------------------------|-----------------|-------|-----|------------------|------------------------|-------------------|--------|------------------|----------------|--------|----------|---------|-----------|----------|----------|----------|-------------|-------------|--------------|-----|----|------------|---------|---------|--------|-------------------------|-------------|-------------|-------------|-------------|---------------|------------|------------|------------|-------------|---------------|--------------|-----------|--------------|
|12072000.001_out_adc.csv|adc0  |[5, 30]        |2020-07-12 06:03:33.303508992|2020-07-12 06:03:34.178714624|0.875414617203008|87     |92   |21.939020872012385|1.7227867928766143      |0.17508292344060156|5422    |113605722116.11168|15              |06:03:19|12.07.2020|33.888535|47.539276  |8405804   |8320944   |8886645   |60019        | 5227.41892 N| 01317.76349 E|1    |8   | 44.0 M     |20.135521|17.200287|74.33371|1638445892749579875966768|8405804      |8320944      |8886645      |256.1339     |10.75          |0.96        |1026.0      |10.13       |2.37         |300.0          |0.01          |0.0        |16.093        |

## Model definition

```kotlin
data class RohDaten(
  val start: LocalTime,
  val end: LocalTime,
  val sensor: Sensor,
  val frequencyRange: FrequencyClassification,
  val temperature: Double,
  val humidity: Double
)

enum class FrequencyClassification {
  TANZEN,
  SCHWIRREN,
  STOPP_SIGNAL,
  WHOOPING,
  FAECHELN
}

enum class Sensor {
  ADC0,
  ADC1,
  ADC2,
  ADC3,
  ADC4,
  ADC5
}
```
## Graph creation definition
```kotlin
      { (data: List<RohDaten>, _, meta) ->
        data
          .groupBy { it.frequencyRange }
          .mapValues { (_, rawData) -> rawData.groupBySensorAndSumOccurenceInEachHourSeperateIntoFrames() }
          .map { (frequencyClassification, data: Map<Sensor, List<Map<Int, Int>>>) ->
            val chart = createDefaultChart("asd")

            data.forEach { (sensor, data) ->
              data.forEachIndexed{index, frame ->
                chart.plotSeries(sensor, frame, {SENSOR_COLORS.getValue(sensor)}, index)
              }
            }

            PlotAndData(
              dir = meta.targetDir(),
              name = "$frequencyClassification-getrennt",
              plot = chart,
              csvHeaders = listOf("Uhrzeit", "Signal/Stunde", "Messsonde"),
              csvRows = data.flatMap { (sensor, d) ->  d.flatMap { it.entries.map { listOf(it.key, it.value, sensor.name) } }}
            )
          }
      }
```
## Graph output
![Graph](https://github.com/maxmesserich93/NoisyCsvVisualizer/blob/master/SCHWIRREN-getrennt.png)

## CSV output (sample)

|Uhrzeit                 |Signal/Stunde|Messsonde|
|------------------------|-------------|---------|
|8                       |14           |ADC1     |
|9                       |15           |ADC1     |
|10                      |9            |ADC1     |
|11                      |15           |ADC1     |
|12                      |7            |ADC1     |
|13                      |5            |ADC1     |
|14                      |8            |ADC1     |
|15                      |40           |ADC1     |
|16                      |31           |ADC1     |
|17                      |21           |ADC1     |
|18                      |44           |ADC1     |
|19                      |50           |ADC1     |
|20                      |98           |ADC1     |
|21                      |47           |ADC1     |
|22                      |9            |ADC1     |
|8                       |8            |ADC3     |
|9                       |10           |ADC3     |
|10                      |2            |ADC3     |
|16                      |1            |ADC3     |
|17                      |15           |ADC3     |
|18                      |37           |ADC3     |




