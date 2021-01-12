import Memoized.Companion.memoized
import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.internal.AtomicOp
import kotlinx.coroutines.withContext
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.io.File
import java.io.FileReader
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference
import kotlin.reflect.KProperty1
import kotlin.reflect.KSuspendFunction1
import kotlin.streams.asSequence


data class Row(val fileName: String, val row: Long, val record: Map<String, String>) {
  fun <T> tryParse(field: String, transform: (String) -> T): Either<RowError, T> {
    return Either.fromNullable(record[field])
      .mapLeft { RowError(fileName, row, "Empty", field, "Missing field $field") }
      .flatMap {
        try {
          transform(it).right()
        } catch (e: Exception) {
          RowError(fileName, row, e::class.simpleName!!, field, e.message ?: "Unknown error").left()
        }
      }
  }
}

val ROW_ERROR_FIELDS: List<KProperty1<RowError, Any>> = listOf(
  RowError::fileName,
  RowError::row,
  RowError::errorType,
  RowError::field,
  RowError::errorMessage
)

data class RowError(
  val fileName: String,
  val row: Long,
  val field: String,
  val errorType: String,
  val errorMessage: String
)

data class ConvertedCsv<T>(val fileName: String, val data: List<T>, val errors: List<String>)

suspend fun <T> prepareDataFromCsvFolder(
  path: Path,
  dataMapper: KSuspendFunction1<Row, Either<RowError, T>>
): Flow<ConvertedCsv<T>> {
//
  return findCsvFilesInFolder(path)
    .asSequence()
    .asFlow()
    .map { it.toConvertedCsv(dataMapper) }
}

private fun findCsvFilesInFolder(path: Path) = Files.walk(path)
  .filter(Files::isRegularFile)
  .filter { it.fileName.toString().endsWith(".csv") }


private suspend fun <T> Path.toConvertedCsv(
  dataMapper: KSuspendFunction1<Row, Either<RowError, T>>
): ConvertedCsv<T> = withContext(Dispatchers.Default) {
  val left = mutableListOf<String>()
  val right = mutableListOf<T>()


  val memoizedPrinter = memoizedCsvPrinterForType(fileName, ROW_ERROR_FIELDS)

  readFile(dataMapper).collect {
    it.fold(
      { error ->
        memoizedPrinter().printer.printRecord(ROW_ERROR_FIELDS.map { kProperty1 -> kProperty1.get(error) })
      },
      right::add
    )
  }
  memoizedPrinter.value?.let(Printer::printer)?.let(CSVPrinter::close)
  ConvertedCsv(fileName.toString(), right, left)
}


private fun <T> memoizedCsvPrinterForType(
  path: Path,
  fields: List<KProperty1<T, Any>>
): Memoized<Printer> = with(
  Path.of(System.getProperty("user.dir"), "errors", "${path.fileName}-errors.csv")){
  {
    println("Found errors in file. ${path.fileName}. Creating error csv: $this" )
    Printer(
      path.fileName.toString(),
      createCsvWithHeaders(this, fields.map { it.name })
    ) }.memoized()
}

private suspend fun <T> Path.readFile(dataMapper: KSuspendFunction1<Row, Either<RowError, T>>): Flow<Either<RowError, T>> =
  CSVFormat.DEFAULT
    .withFirstRecordAsHeader()
    .parse(FileReader(toFile()))
    .asFlow()
    .map { csvRecord ->
      Row(
        fileName = fileName.toString(),
        row = csvRecord.recordNumber,
        record = csvRecord.toMap()
      )
    }
    .map { dataMapper.invoke(it) }


fun createCsvWithHeaders(path: Path, headers: List<String>): CSVPrinter{

  File(path.parent.toUri()).mkdirs()
  Files.deleteIfExists(path)
  val createFile: Path = Files.createFile(path)



  return CSVPrinter(
    Files.newBufferedWriter(createFile),
    CSVFormat.DEFAULT.withHeader(*headers.toTypedArray())
  )

}

data class Printer(val fileName: String, val printer: CSVPrinter)

 class Memoized<T>(private val reference: AtomicReference<T?> = AtomicReference(), private val fn: () -> T): () -> T{
   companion object{
      fun <T> (() -> T).memoized() = Memoized(reference = AtomicReference(), this)
   }

   val value : T?
     get() = reference.acquire

   override fun invoke(): T = when {
     reference.get() == null -> {
       reference.updateAndGet { fn() }!!
     }else -> reference.get()!!;
   }
 }






