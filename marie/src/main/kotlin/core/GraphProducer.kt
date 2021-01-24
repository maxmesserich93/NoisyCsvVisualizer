import arrow.core.Either
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.withContext
import org.apache.commons.csv.CSVPrinter
import org.knowm.xchart.BitmapEncoder
import org.knowm.xchart.internal.chartpart.Chart
import java.io.File
import java.nio.file.Path

data class DataFileName<T>(val data: List<T>, val errors: List<RowError>, val meta: Meta)
data class PlotAndData(
  val dir: Path,
  val name: String,
  val plot: Chart<*,*>,
  val csvHeaders: List<String>,
  val csvRows: List<List<Comparable<*>>>
)


data class Meta(
  val dir: String,
  val datePrefix: String,
  val path: Path,
  val targetBaseDir: Path
){
  fun targetDir(): Path = targetBaseDir.resolve(datePrefix)
}



suspend fun <T> produceGraphs(
  sourceCSVFolder: Path,
  outputFolder: Path,
  csvSeparator: Char,
  dataParser: suspend (Row) -> Either<RowError, T>,
  filter: (T) -> Boolean,
  graphCreationFunctions: List<(DataFileName<T>) -> Collection<PlotAndData>>
) {
  coroutineScope {
    withContext(Dispatchers.Default) {
      createActor<Path> { path, main ->
        findCsvFilesInFolder(path.value).asFlow().collect { path ->
            main.createChild<Path> { path, actor ->
              val (fileName, data, errors) = path.value.toConvertedCsv(csvSeparator,dataParser)

              val datePrefix = fileName.take(4)
              val meta = Meta(fileName, datePrefix, path.value, outputFolder)
              val dataFileName = DataFileName(data.filter(filter), errors, meta)

              val error = actor.createChild(::errorWriter).ref()

              error.sendPayload(dataFileName)
              actor.createChild<DataFileName<T>> { payload, actor ->
                processor(
                  payload.value,
                  actor,

                  graphCreationFunctions
                )
              }.ref().sendPayload(dataFileName)

            }.ref().sendPayload(path)
          }
      }.ref().sendPayload(sourceCSVFolder)
    }
  }
}

private suspend fun errorWriter(a: Payload<DataFileName<*>>, b: ActorData<DataFileName<*>>) {
  //TODO memoized is no longer required
  val memoizedPrinter = memoizedErrorPrinter(a.value.meta.path, ROW_ERROR_FIELDS)
  a.value.errors.forEach { error ->
    memoizedPrinter().printer
      .printRecord(ROW_ERROR_FIELDS.map { kProperty1 -> kProperty1.get(error) }
      )
  }
  memoizedPrinter.value?.let(Printer::printer)?.let(CSVPrinter::close)
  b.finished()
}

suspend inline fun <T> DataFileName<T>.createPlot(
  actorData: ActorData<DataFileName<T>>,
  graphFunctions: List<(DataFileName<T>) -> Collection<PlotAndData>>
) {
  graphFunctions.map { fn ->
    actorData.createChild<DataFileName<T>> { a, b ->
      fn(a.value).forEach { it.saveGraphAndData() }
      b.finished()
    }.ref()
  }.forEach { it.sendPayload(this) }
}

suspend fun <T> processor(
  message: DataFileName<T>,
  actorData: ActorData<DataFileName<T>>,
  list: List<(DataFileName<T>) -> Collection<PlotAndData>>
) = when {
  message.data.isEmpty() -> actorData.finished()
  else -> message.createPlot(actorData, list)
}

suspend fun PlotAndData.saveGraphAndData() {
//  coroutineScope {
//    withContext(Dispatchers.IO) {
      File(dir.parent.toUri()).mkdirs()
      createCsvFile(csvHeaders, csvRows, dir.resolve("$name.csv"))

      BitmapEncoder
        .saveBitmapWithDPI(plot,
          dir.resolve(name).toString(),
          BitmapEncoder.BitmapFormat.PNG,
          200);

//    }
//  }
}

typealias GraphComponent = List<*>
typealias Mapping = Pair<String, () -> GraphComponent>

private fun createCsvFile(headers: List<String>, data: List<List<Comparable<*>>>, csvFile: Path) {
  val csv = createCsvWithHeaders(
    csvFile,
    headers
  )


  data.forEach {
    val asIterable: Iterable<Any> = it.asIterable()
    csv.printRecord(asIterable)

  }

  csv.close(true)
}

fun calculateGraphData(
  x_axis: Mapping,
  y_axis: Mapping,
  grouping: Mapping? = null,
  colorMapping: Mapping? = null
): Map<String, GraphComponent> = arrayOf(
  x_axis,
  y_axis,
  grouping,
  colorMapping
)
  .filterNotNull()
  .map { (key, sup) -> key to sup() }
  .toMap()
