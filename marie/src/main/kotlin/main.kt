import FrequencyClassification.SCHWIRREN
import FrequencyClassification.TANZEN
import arrow.core.extensions.list.monad.flatten
import jetbrains.letsPlot.export.ggsave
import jetbrains.letsPlot.geom.geom_line
import jetbrains.letsPlot.ggsize
import jetbrains.letsPlot.intern.Plot
import jetbrains.letsPlot.lets_plot
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths



suspend fun main() {

  prepareDataFromCsvFolder(
    path = Paths.get(".", "data"),
    dataMapper = ::mapToData
  )
    .map {
      println("${it.fileName}: ${it.data.size}")
      it
    }
    .filter { it.data.isNotEmpty() }
    .collect { convertedCsv ->
      val datePrefix = convertedCsv.fileName.take(4)
      val groupedByFrequency: Map<FrequencyClassification, List<RohDaten>> =
        convertedCsv.data.groupBy { it.frequencyRange }

      val dir = "graphs/$datePrefix"
      File(System.getProperty("user.dir"), dir).mkdirs()

      coroutineScope {
        withContext(Dispatchers.Default) {
          groupedByFrequency.mapValues { (frequencyClassification, rawData) ->
            rawData.sumOccurenceInEachHour().plotSum()
          }.map { (frequencyClassification, graph) ->
            ggsave(graph, filename = "$frequencyClassification-${"summiert"}.png", path = dir)
          }
        }

        withContext(Dispatchers.Default) {
          groupedByFrequency
            .mapValues { (_, rawData) ->
              rawData.groupBySensorAndSumOccurenceInEachHour().plot()
            }.map { (frequencyClassification, graph) ->
              ggsave(graph, filename = "$frequencyClassification-${"getrennt"}.png", path = dir)
            }
        }
        withContext(Dispatchers.Default) {
          groupedByFrequency
            .filterKeys { it == TANZEN || it == SCHWIRREN }
            .flatMap { it.value }
            .let { rawData -> rawData.sumOccurenceInEachHour().plotSum() }
            .let { graph -> ggsave(graph, filename = "T+S.png", path = dir) }
        }

      }
    }
}


fun List<RohDaten>.sumOccurenceInEachHour(): Map<Int, Int> =
  this.groupBy { it.start.hour.toInt() }.mapValues { it.value.count() }


fun Map<Int, Int>.plotSum(): Plot {

  val data = mapOf(
    "Uhrzeit" to keys.toList(),
    "Anzahl Signal/Stunde" to values.toList(),
  )

  var plot = lets_plot(data)

  plot += geom_line(color = "black") { x = "Uhrzeit"; y = "Anzahl Signal/Stunde" }
  plot + ggsize(500, 250)

  return plot


}

fun List<RohDaten>.groupBySensorAndSumOccurenceInEachHour(): Map<Sensor, Map<Int, Int>> =
  this
    .groupBy { it.sensor }
    .mapValues { it.value.sumOccurenceInEachHour() }

fun Map<Sensor, Map<Int, Int>>.plot(): Plot {
  val map = this.map { e ->
    mapOf(
      "Uhrzeit" to e.value.map { it.key },
      "Anzahl Signal/Stunde" to e.value.map { it.value },
      "Sensor" to e.value.map { e.key.name },
      "Messsonde" to e.value.map { e.key.name }
    )
  }

  val groupBy: Map<String, List<*>> = map
    .flatMap { it.asSequence() }
    .groupBy { it.key }
    .mapValues { it.value.map { a -> a.value }.flatten() }

  var plot = lets_plot(groupBy)

  plot += geom_line {
    x = "Uhrzeit"; y = "Anzahl Signal/Stunde"; group = "Sensor"; color = "Messsonde"
  }
  plot + ggsize(500, 250)

  return plot

}









