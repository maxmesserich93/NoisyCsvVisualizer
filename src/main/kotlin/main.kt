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
    val humidity: Double,
    val outsideHumidity: Double,
    val outsideTemperature: Double,

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
    produceGraphs(
        sourceCSVFolder = Paths.get("example"),
        outputFolder = Paths.get("graphs"),
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
                    humidity = !row.tryParse("humidity_in", String::toDouble),
                    outsideHumidity = !row.tryParse("fio_humidity", String::toDouble),
                    outsideTemperature = !row.tryParse("fio_temperature", String::toDouble)
                )
            }
        },
        //Remove irrelevant hours
        filter = { rohDatenSatz -> rohDatenSatz.start.hour in 8..23 },
        //Define a list of functions producing n graphs. All CSV files from the source folder are applied to these functions
        graphCreationFunctions = listOf(
            { (data: List<RohDaten>, _, meta) ->
                data
                    .groupBy { it.frequencyRange }
                    .mapValues { (_, rawData) -> rawData.sumOccurenceInEachHourSeperateIntoFrames() }
                    .map { (frequency, data: List<Map<Int, Int>>) ->
                        val chart = createDefaultChart("asd", "Uhrzeit", "Signal/Stunde")
                        //Hier wird der style gesetzt.
                        chart.withWhiteBackground()
                        data.forEachIndexed { index, range ->
                            chart.plotSeries(
                                frequency,
                                range,
                                { FREQUENCY_COLORS.getValue(frequency) },
                                index
                            )
                        }

                        PlotAndData(
                            dir = meta.targetDir(),
                            name = "${frequency}-summiert",
                            plot = chart,
                            csvHeaders = listOf("Uhrzeit", "Signal/Stunde"),
                            csvRows = data.flatMap { range ->
                                range.entries.map {
                                    listOf(
                                        it.key,
                                        it.value,
                                        frequency.name
                                    )
                                }
                            }
                        )
                    }
            },

            { (data: List<RohDaten>, _, meta) ->
                data
                    .groupBy { it.frequencyRange }
                    .mapValues { (_, rawData) -> rawData.groupBySensorAndSumOccurenceInEachHourSeperateIntoFrames() }
                    .map { (frequencyClassification, data: Map<Sensor, List<Map<Int, Int>>>) ->
                        val chart = createDefaultChart("asd", "Uhrzeit", "Signal/Stunde")

                        data.forEach { (sensor, data) ->
                            data.forEachIndexed { index, frame ->
                                chart.plotSeries(
                                    sensor,
                                    frame,
                                    { SENSOR_COLORS.getValue(sensor) },
                                    index
                                )
                            }
                        }

                        PlotAndData(
                            dir = meta.targetDir(),
                            name = "$frequencyClassification-getrennt",
                            plot = chart,
                            csvHeaders = listOf("Uhrzeit", "Signal/Stunde", "Messsonde"),
                            csvRows = data.flatMap { (sensor, d) ->
                                d.flatMap {
                                    it.entries.map {
                                        listOf(
                                            it.key,
                                            it.value,
                                            sensor.name
                                        )
                                    }
                                }
                            }
                        )
                    }
            },

            { (data: List<RohDaten>, _, meta) ->
                val tempByHour: Map<Int, Double> = data
                    .groupBy { it.start.hour }
                    .mapValues { (a, b) -> b.map(RohDaten::temperature).average() }

                val chart = createDefaultChart("asd", "Uhrzeit", "Signal/Stunde")
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

                val chart = createDefaultChart("asd", "Uhrzeit", "Signal/Stunde")

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

                val chart = createDefaultChart("asd", "Uhrzeit", "Signal/Stunde")
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

                val signalsByType: Map<FrequencyClassification, List<Map<Int, Int>>> = data
                    .groupBy(RohDaten::frequencyRange)
                    .mapValues { (_, data) -> data.sumOccurenceInEachHourSeperateIntoFrames() }

                val chart: XYChart =
                    createDefaultChart("TITEL TESEN TESTEN", "Uhrzeit", "Signal/Stunde")

                signalsByType.forEach { (freq, data) ->
                    data.forEachIndexed { index, range ->
                        chart.plotSeries(freq, range, { FREQUENCY_COLORS.getValue(freq) }, index)
                    }

                }

                listOf(
                    PlotAndData(
                        dir = meta.targetDir(),
                        name = "alle",
                        plot = chart,
                        csvHeaders = listOf("Uhrzeit", "Signal/Stunde", "Frequenz"),
                        csvRows = signalsByType.flatMap { (freq, data) ->
                            data.flatMap {
                                it.entries.map {
                                    listOf(
                                        it.key,
                                        it.value,
                                        freq.name
                                    )
                                }
                            }
                        }
                    )
                )

            },
            { (data: List<RohDaten>, _, meta) ->

                val humidityInside =
                    data.groupBy { it.start.hour }
                        .mapValues { (a, b) -> b.map(RohDaten::humidity).average() }

                val humidityOutside =
                    data.groupBy { it.start.hour }
                        .mapValues { (a, b) -> b.map(RohDaten::outsideHumidity).average() }


                val chart = createDefaultChart(
                    "Humidity inside and outside",
                    "Uhrzeit",
                    "Signal/Stunde"
                )

                chart.addSeries(
                    "humidity inside",
                    humidityInside.keys.toList(),
                    humidityInside.values.toList()
                )

                chart.addSeries(
                    "humidity outside",
                    humidityOutside.keys.toList(),
                    humidityOutside.values.toList()
                )

                listOf(
                    PlotAndData(
                        dir = meta.targetDir(),
                        name = "humidity_inside_outside",
                        plot = chart,
                        csvHeaders = listOf("Time", "humidity_inside", "humidity_outside"),
                        csvRows = humidityInside.map { listOf(it.key, it.value, humidityOutside[it.key]!!) }
                    )
                )
            },
            { (data: List<RohDaten>, _, meta) ->

                val temperatureInside =
                    data.groupBy { it.start.hour }
                        .mapValues { (a, b) -> b.map(RohDaten::temperature).average() }

                val temperatureOutside =
                    data.groupBy { it.start.hour }
                        .mapValues { (a, b) -> b.map(RohDaten::outsideTemperature).average() }


                val chart = createDefaultChart(
                    "Temperature inside and outside",
                    "time",
                    "signales/hour"
                )

                chart.addSeries(
                    "temperature inside",
                    temperatureInside.keys.toList(),
                    temperatureInside.values.toList()
                )

                chart.addSeries(
                    "temperature outside",
                    temperatureOutside.keys.toList(),
                    temperatureOutside.values.toList()
                )

                listOf(
                    PlotAndData(
                        dir = meta.targetDir(),
                        name = "temperature_inside_outside",
                        plot = chart,
                        csvHeaders = listOf("Time", "temperature_inside", "temperature_outside"),
                        csvRows = temperatureInside.map { listOf(it.key, it.value, temperatureOutside[it.key]!!) }
                    )
                )
            }
        )
    )
}

fun <T : Enum<T>> XYChart.plotSeries(
    key: Enum<T>,
    data: Map<Int, Int>,
    color: (Enum<T>) -> Color,
    rangeId: Int? = null
) {
    val series = addSeries(rangeId?.let { "${key.name}_$rangeId" } ?: key.name,
        data.keys.toList(), data.values.toList())
    series.lineColor = color(key)
}

private fun createDefaultChart(title: String, xAxisTitle: String, yAxisTitle: String): XYChart {
    return XYChartBuilder()
        .width(800)
        .height(600)
        .title(title)
        .xAxisTitle(xAxisTitle)
        .yAxisTitle(yAxisTitle)
        .build()
        .withWhiteBackground()
}

private fun XYChart.withWhiteBackground(): XYChart = this.apply {
    styler.plotBackgroundColor = Color.WHITE
    styler.plotGridLinesColor = Color.WHITE
}

fun List<RohDaten>.sumOccurenceInEachHourSeperateIntoFrames(): List<Map<Int, Int>> {
    val byHour = this.groupBy { it.start.hour }.mapValues { it.value.count() }
    val frames = findConsecutiveIntFrames(byHour.keys)

    return frames.map { it.map { hour -> hour to byHour[hour]!! }.toMap() }
}

fun List<RohDaten>.groupBySensorAndSumOccurenceInEachHourSeperateIntoFrames(): Map<Sensor, List<Map<Int, Int>>> =
    this
        .groupBy { it.sensor }
        .mapValues { it.value.sumOccurenceInEachHourSeperateIntoFrames() }


interface OP {
    val accumeator: List<List<Int>>
    val current: List<Int>
}

interface WithValue {
    val lastValue: Int
}

sealed class State(override val accumeator: List<List<Int>>) : OP
data class Empty(override val accumeator: List<List<Int>>, override val current: List<Int>) :
    State(accumeator)

data class NonEmpty(
    override val accumeator: List<List<Int>>,
    override val current: List<Int>,
    override val lastValue: Int
) : State(accumeator), WithValue

fun findConsecutiveIntFrames(ints: Set<Int>): List<List<Int>> {
    val op = ints.sorted().fold(
        Empty(mutableListOf(), emptyList()) as State,
        operation = { acc, value ->
            when (acc) {
                is Empty -> NonEmpty(acc.accumeator, listOf(value), value)
                is NonEmpty -> {
                    value - acc.lastValue
                    when (value - acc.lastValue) {
                        1 -> NonEmpty(
                            accumeator = acc.accumeator,
                            current = acc.current + value,
                            lastValue = value
                        )
                        else -> NonEmpty(
                            accumeator = (acc.accumeator + listOf(acc.current)),
                            listOf(value),
                            value
                        )
                    }
                }
            }
        }
    )
    return when {
        op.current.isNotEmpty() -> (op.accumeator + listOf(op.current))
        else -> op.accumeator
    }
}
