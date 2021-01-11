import FrequencyClassification.*
import arrow.core.Either
import arrow.core.computations.either
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter

data class RohDaten(
  val start: LocalTime,
  val end: LocalTime,
  val sensor: Sensor,
  val frequencyRange: FrequencyClassification
)

fun String.classifySensor() = when (this) {
  "adc0" -> Sensor.ADC0
  "adc1" -> Sensor.ADC1
  "adc2" -> Sensor.ADC2
  "adc3" -> Sensor.ADC3
  "adc4" -> Sensor.ADC4
  "adc5" -> Sensor.ADC5
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




//HASS
fun parseDate(entry: String): LocalDateTime = if (entry.contains("T")) {
  LocalDateTime.parse(entry, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
} else {
  val suffixlen = entry.length - entry.indexOfLast { it == '.' }
  val pattern = "yyyy-MM-dd HH:mm:ss.${(2..suffixlen).joinToString(separator = "") { "S" }}"
  LocalDateTime.parse(entry, DateTimeFormatter.ofPattern(pattern))
}

suspend fun mapToData(input: Row): Either<RowError, RohDaten> = either {
  RohDaten(
    start = !input.tryParse("begin_datetime_utc", ::parseDate).map { it.toLocalTime() },
    end = !input.tryParse("end_datetime_utc", ::parseDate).map { it.toLocalTime() },
    sensor = !input.tryParse("adc_ch", String::classifySensor),
    frequencyRange = !input.tryParse("frequency_range", String::classifyFrequency)
  )
}
