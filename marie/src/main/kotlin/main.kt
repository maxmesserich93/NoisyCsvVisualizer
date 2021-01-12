import jetbrains.letsPlot.export.ggsave
import jetbrains.letsPlot.geom.geom_line
import jetbrains.letsPlot.ggsize
import jetbrains.letsPlot.intern.Feature
import jetbrains.letsPlot.intern.Plot
import jetbrains.letsPlot.lets_plot
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.transform
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths

data class PlotAndData(
  val dir: String,
  val name: String,
  val plot: Plot,
  val keys: List<String>,
  val data: List<List<Comparable<*>>>
  )

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
    .transform { convertedCsv ->
      val datePrefix = convertedCsv.fileName.take(4)
      val groupedByFrequency: Map<FrequencyClassification, List<RohDaten>> =
        convertedCsv.data.groupBy { it.frequencyRange }

      val dir = "graphs/$datePrefix"
      File(System.getProperty("user.dir"), dir).mkdirs()

      //TODO: FIX BESCHRIFTUNG
      val summiert = {groupedByFrequency
        .mapValues { (frequencyClassification, rawData) -> rawData.sumOccurenceInEachHour() }
        .map { (frequencyClassification, data) ->
          val x_axis = "Uhrzeit" to { data.keys.toList() }
          val y_axis = "Anzahl Signal/Stunde" to { data.values.toList() }

          val colorMapping = "Messsonde" to { List(data.size) { frequencyClassification.name } }


          val graphData = calculateGraphData(x_axis, y_axis, colorMapping = colorMapping)
          PlotAndData(
            dir,
            "$frequencyClassification-summiert",
            plot = graphData.createGraph(geomLine(x_axis, y_axis, colorMapping)) + ggsize(500, 250),
            graphData.keys.toList(),
            graphData.map { it.value }.rotate()
          )
        }}


      val getrennt: () -> List<PlotAndData> = {groupedByFrequency
        .mapKeys { (frequencyClassification, _) -> "$frequencyClassification-getrennt" }
        .mapValues { (_, rawData) -> rawData.groupBySensorAndSumOccurenceInEachHour() }
        .map { (name, data) ->

          val x_axis = "Uhrzeit" to { data.values.flatMap { it.keys } }
          val y_axis = "Anzahl Signal/Stunde" to { data.values.flatMap { it.values } }
          val sensorPerGraph = { data.flatMap { (sensor, d) -> List(d.size) { sensor.name } } }
          val colorMapping = "Messsonde" to sensorPerGraph

          val graphData = calculateGraphData(
            x_axis,
            y_axis,
            colorMapping
          )
          PlotAndData(
            dir,
            name,
            graphData.createGraph(geomLine(x_axis, y_axis, colorMapping = colorMapping)) + ggsize(500, 250),
            graphData.keys.toList(),
            graphData.map { it.value }.rotate()
          )
        }
      }

      emit(getrennt)
      emit(summiert)
    }
    .buffer()
    .flatMapMerge { operations: () -> List<PlotAndData> -> coroutineScope { operations() }.asFlow() }
    .collect{ it.saveGraphAndData() }
}

private fun PlotAndData.saveGraphAndData(){
  ggsave(plot, filename = "$name.png", path = dir)
  createCsvFile(keys, data, Path.of(dir, "$name.csv"))
}

private fun createCsvFile(headers: List<String>, data: List<List<Comparable<*>>>, csvFile: Path) {
  val csv = createCsvWithHeaders(
    csvFile,
    headers
  )
  data.forEach { csv.printRecord(it) }
  csv.close(true)
}

private fun List<List<Comparable<*>>>.rotate(): List<List<Comparable<*>>> =
  (0 until (this.firstOrNull()?.size ?: 0)).map { index -> map { it[index] } }

fun List<RohDaten>.sumOccurenceInEachHour(): Map<Int, Int> =
  this.groupBy { it.start.hour.toInt() }.mapValues { it.value.count() }


fun List<RohDaten>.groupBySensorAndSumOccurenceInEachHour(): Map<Sensor, Map<Int, Int>> =
  this
    .groupBy { it.sensor }
    .mapValues { it.value.sumOccurenceInEachHour() }



typealias Mapping = Pair<String, () -> List<Comparable<*>>>


fun calculateGraphData(
  x_axis: Mapping,
  y_axis: Mapping,
  grouping: Mapping? = null,
  colorMapping: Mapping? = null
): Map<String, List<Comparable<*>>> = arrayOf(
  x_axis,
  y_axis,
  grouping,
  colorMapping
)
  .filterNotNull()
  .map { (key, sup) -> key to sup() }
  .toMap()

private fun geomLine(
  x_axis: Mapping,
  y_axis: Mapping,
  grouping: Mapping? = null,
  colorMapping: Mapping? = null
): Feature = geom_line {
  x = x_axis.first; y = y_axis.first; group = grouping?.first; color = colorMapping?.first
}

private fun geomLineBlack(
  x_axis: Mapping,
  y_axis: Mapping,
  color: Any
): Feature = geom_line(color = color) {
  x = x_axis.first; y = y_axis.first;
}


fun Map<Sensor, Map<Int, Int>>.plot(): Plot {

  val x_axis = "Uhrzeit" to { this.values.flatMap { it.keys } }
  val y_axis = "Anzahl Signal/Stunde" to { this.values.flatMap { it.values } }
  val sensorPerGraph =
    { entries.flatMap { (sensor, data) -> (0 until data.size).map { sensor.name } } }
  val colorMapping = "Messsonde" to sensorPerGraph

  val graphData = calculateGraphData(
    x_axis,
    y_axis,
    colorMapping
  )

  return graphData.createGraph(geomLine(x_axis, y_axis, colorMapping = colorMapping))
}


fun Map<String, List<*>>.createGraph(lineMapping: Feature): Plot {
  var plot = lets_plot(this)
  plot += lineMapping
  plot + ggsize(500, 250)
  return plot

}
