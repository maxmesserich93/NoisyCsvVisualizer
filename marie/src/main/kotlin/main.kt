import FrequencyClassification.FAECHELN
import FrequencyClassification.SCHWIRREN
import FrequencyClassification.STOPP_SIGNAL
import FrequencyClassification.TANZEN
import FrequencyClassification.WHOOPING
import Sensor.ADC0
import Sensor.ADC1
import Sensor.ADC2
import Sensor.ADC3
import Sensor.ADC4
import Sensor.ADC5
import arrow.core.computations.either
import org.knowm.xchart.XYChart
import org.knowm.xchart.XYChartBuilder
import java.awt.Color
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter


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


fun String.classifySensor() = when (this) {
  "adc0" -> ADC0
  "adc1" -> ADC1
  "adc2" -> ADC2
  "adc3" -> ADC3
  "adc4" -> ADC4
  "adc5" -> ADC5
  else -> throw IllegalArgumentException("UNKNOWN SENSOR: $this")
}

fun String.classifyFrequency(): FrequencyClassification = when (this) {
  "[5, 30]" -> TANZEN
  "[190, 230]" -> SCHWIRREN
  "[380, 400]" -> STOPP_SIGNAL
  "[250, 370]" -> WHOOPING
  "[90, 120]" -> FAECHELN
  else -> throw IllegalArgumentException("$this is not a valid frequency")
}


val SENSOR_COLORS = mapOf(
  ADC0 to Color.BLUE,
  ADC1 to Color.BLACK,
  ADC2 to Color.RED,
  ADC3 to Color.YELLOW,
  ADC4 to Color.GREEN,
  ADC5 to Color.CYAN
)

val FREQUENCY_COLORS = mapOf(
  TANZEN to Color.BLUE,
  SCHWIRREN to Color.BLACK,
  STOPP_SIGNAL to Color.RED,
  WHOOPING to Color.YELLOW,
  FAECHELN to Color.GREEN
)

//HASS
fun parseDate(entry: String): LocalDateTime = if (entry.contains("T")) {
  LocalDateTime.parse(entry, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
} else {
  val suffixlen = entry.length - entry.indexOfLast { it == '.' }
  val pattern = "yyyy-MM-dd HH:mm:ss.${(2..suffixlen).joinToString(separator = "") { "S" }}"
  LocalDateTime.parse(entry, DateTimeFormatter.ofPattern(pattern))
}


suspend fun main() {
  System.setProperty("org.graphstream.ui", "swing");
  SYSTEM.renderer.display()
  produceGraphs(
    sourceCSVFolder = Paths.get(".", "example"),
    outputFolder = Paths.get(".", "graphs"),
    csvSeparator = ',',
    //Convert csv row to your data type
    dataParser = { row ->
      either {
        RohDaten(
          //Do not question the addition of 2 hours!
          start = !row
            .tryParse("begin_datetime_utc", ::parseDate)
            .map { it.toLocalTime().plusHours(2) },
          end = !row.tryParse("end_datetime_utc", ::parseDate)
            .map { it.toLocalTime().plusHours(2) },
          sensor = !row.tryParse("adc_ch", String::classifySensor),
          frequencyRange = !row.tryParse("frequency_range", String::classifyFrequency),
          temperature = !row.tryParse("temp_in", String::toDouble),
          humidity = !row.tryParse("humidity_in", String::toDouble)
        )
      }
    },
    //Remove all entries for Sensor ADC5 because science.
    filter = { true },
    //Define a list of functions producing n graphs. All CSV files from the source folder are applied to these functions
    graphCreationFunctions = listOf(
      { (data: List<RohDaten>, _, meta) ->
        data
          .groupBy { it.frequencyRange }
          .mapValues { (_, rawData) -> rawData.sumOccurenceInEachHour() }
          .map { (frequency, data: Map<Int, Int>) ->
            val chart = createDefaultChart("asd")
            chart.plotSeries(frequency, data) { FREQUENCY_COLORS.getValue(frequency) }

            PlotAndData(
              dir = meta.targetDir(),
              name = "${frequency}-summiert",
              plot = chart,
              csvHeaders = listOf("Uhrzeit", "Signal/Stunde"),
              csvRows = data.map { (hour, occurence) -> listOf(hour, occurence) }
            )
          }
      },

      { (data: List<RohDaten>, _, meta) ->
        data
          .groupBy { it.frequencyRange }
          .mapValues { (_, rawData) -> rawData.groupBySensorAndSumOccurenceInEachHour() }
          .map { (frequencyClassification, data) ->
            val chart = createDefaultChart("asd")

            data.forEach { (sensor, data) ->
              chart.plotSeries(sensor, data) { SENSOR_COLORS.getValue(sensor) }
            }

            PlotAndData(
              dir = meta.targetDir(),
              name = "$frequencyClassification-getrennt",
              plot = chart,
              csvHeaders = listOf("Uhrzeit", "Signal/Stunde", "Messsonde"),
              csvRows = data.flatMap { (sensor, data) ->
                data.map {
                  listOf(
                    it.key,
                    it.value,
                    sensor
                  )
                }
              }
            )
          }
      },

      { (data: List<RohDaten>, _, meta) ->
        val tempByHour: Map<Int, Double> = data
          .groupBy { it.start.hour }
          .mapValues { (a, b) -> b.map(RohDaten::temperature).average() }

        val chart = createDefaultChart("asd")
        chart.addSeries(
          "Temperatur",
          tempByHour.keys.toList(),
          tempByHour.values.toList()
        )

        listOf(PlotAndData(
          dir = meta.targetDir(),
          name = "temperatur",
          plot = chart,
          csvHeaders = listOf("Uhrzeit", "Temperatur"),
          csvRows = tempByHour.map { (hour, temp) -> listOf(hour, temp) }
        ))

      },
      { (data: List<RohDaten>, _, meta) ->

        val tempByHour =
          data.groupBy { it.start.hour }
            .mapValues { (a, b) -> b.map(RohDaten::humidity).average() }

        val chart = createDefaultChart("asd")

        chart.addSeries(
          "Luftfeuchtigkeit",
          tempByHour.keys.toList(),
          tempByHour.values.toList()
        )

        listOf(
          PlotAndData(
            dir = meta.targetDir(),
            name = "luftfeuchtigkeit",
            plot = chart,
            csvHeaders = listOf("Uhrzeit", "Temperatur"),
            csvRows = tempByHour.map { listOf(it.key, it.value) }
          )
        )
      },
      { (data, _, meta) ->
        val totalSignals: Map<Int, Int> =
          data.groupBy { it.start.hour }.mapValues { (_, b) -> b.size }

        val chart = createDefaultChart("asd")
        chart.addSeries("Signale", totalSignals.keys.toList(), totalSignals.values.toList())

        listOf(
          PlotAndData(
            dir = meta.targetDir(),
            name = "signale_pro_stunde",
            plot = chart,
            csvHeaders = listOf("Uhrzeit", "Signal/Stunde"),
            csvRows = totalSignals.map { listOf(it.key, it.value) }
          )
        )

      },
      { (data, _, meta) ->

        val signalsByType: Map<FrequencyClassification, Map<Int, Int>> = data
          .groupBy(RohDaten::frequencyRange)
          .mapValues { (_, data) -> data.sumOccurenceInEachHour() }

        val chart: XYChart =
          createDefaultChart("asd")

        signalsByType.forEach { (freq, data) ->
          chart.plotSeries(freq, data) { FREQUENCY_COLORS.getValue(freq) }
        }


        listOf(
            PlotAndData(
              dir = meta.targetDir(),
              name = "alle",
              plot = chart,
              csvHeaders = listOf("Uhrzeit", "Signal/Stunde", "Frequenz"),
              csvRows = signalsByType.flatMap { (freq,data) -> data.map { listOf(it.key, it.value, freq) } }
            )
        )

      }
    )
  )
}

fun <T : Enum<T>> XYChart.plotSeries(
  key: Enum<T>,
  data: Map<Int, Int>,
  color: (Enum<T>) -> Color
) {
  val series = addSeries(key.name, data.keys.toList(), data.values.toList())
  series.lineColor = color(key)
}

private fun createDefaultChart(title: String): XYChart {
  return XYChartBuilder()
    .width(800)
    .height(600)
    .title(title)
    .xAxisTitle("Uhrzeit")
    .yAxisTitle("Signal/Stunde")
    .build()!!
}

fun List<RohDaten>.sumOccurenceInEachHour(): Map<Int, Int> =
  this.groupBy { it.start.hour }.mapValues { it.value.count() }


fun List<RohDaten>.groupBySensorAndSumOccurenceInEachHour(): Map<Sensor, Map<Int, Int>> =
  this
    .groupBy { it.sensor }
    .mapValues { it.value.sumOccurenceInEachHour() }

