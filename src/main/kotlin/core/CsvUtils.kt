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
import kotlinx.coroutines.withContext
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.io.File
import java.io.FileReader
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Stream
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

data class ConvertedCsv<T>(val fileName: String, val data: List<T>, val errors: List<RowError>)

fun findCsvFilesInFolder(path: Path): Sequence<Path> = Files.walk(path).asSequence()
  .filter(Files::isRegularFile)
  .filter { it.fileName.toString().endsWith(".csv") }



suspend fun <T> Path.toConvertedCsv(
  seperator: Char,
  dataMapper: suspend (Row) -> Either<RowError, T>,
): ConvertedCsv<T> = withContext(Dispatchers.Default) {
  val left = mutableListOf<RowError>()
  val right = mutableListOf<T>()

  readFile(seperator, dataMapper).collect {
    it.fold(
      left::add,
      right::add
    )
  }
  ConvertedCsv(fileName.toString(), right, left)
}


 fun <T> memoizedErrorPrinter(
  path: Path,
  fields: List<KProperty1<T, Any>>
): Memoized<Printer> = with(
  Paths.get(System.getProperty("user.dir"), "errors", "${path.fileName}-errors.csv")){
  {
    println("Found errors in file. ${path.fileName}. Creating error csv: $this" )
    Printer(
      path.fileName.toString(),
      createCsvWithHeaders(this, fields.map { it.name })
    ) }.memoized()
}

private suspend fun <T> Path.readFile(seperator: Char,dataMapper: suspend (Row) -> Either<RowError, T>): Flow<Either<RowError, T>> =
  CSVFormat.DEFAULT
    .withDelimiter(seperator)
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
    .map { dataMapper(it) }


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

 public class Memoized<T>(private val reference: AtomicReference<T?> = AtomicReference(), private val fn: () -> T): () -> T{
   companion object{
      fun <T> (() -> T).memoized() = Memoized(reference = AtomicReference(), this)
   }

   val value : T?
     get() = this.reference.get()

   override fun invoke(): T = when {
     reference.get() == null -> {
       reference.updateAndGet { fn() }!!
     }else -> reference.get()!!
   }
 }






