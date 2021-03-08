import MessageSource.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.reflect.KClass

//Very stupid way to keep track of some stuff
data class ActorSystem(
  val idCount: AtomicInteger,
) {
  fun getId() = idCount.incrementAndGet()
}

val SYSTEM = ActorSystem(
  AtomicInteger(0))

enum class ActorState {
  STARTED,
  FINISHED,
}

enum class MessageSource {
  Parent,
  Child,
  Unknown
}

data class ActorRef<T>(
  val clazz: KClass<*>,
  val actorMessages: SendChannel<ActorMessage<*>>,
  val business: SendChannel<Payload<T>>,
  val id: Int,
  val state: ActorState

) {
  suspend fun sendPayload(content: T) {
    business.send(Payload(content))
  }

  suspend fun sendMessage(message: ActorMessage<*>) {
    actorMessages.send(message)
  }
}

data class ActorData<T>(
  val clazz: KClass<*>,
  val inboundActorMessages: Channel<ActorMessage<*>>,
  val inboundBusinessChannel: Channel<Payload<T>>,
  var parent: ActorRef<*>?,
  private val children: MutableMap<Int, ActorRef<*>> = mutableMapOf(),
  override val coroutineContext: CoroutineContext,
  val id: Int,
  var state: ActorState = ActorState.STARTED
) : CoroutineScope {
  /**
   * Processes messages from both channels sequentially.
   *
   * @param payload
   * @param message
   */
  @FlowPreview
  suspend fun listener(
    payload: suspend (Payload<T>, ActorData<T>) -> Unit,
    message: suspend (ActorMessage<*>) -> Unit
  ) {
    this.launch {
      flowOf(inboundActorMessages, inboundBusinessChannel)
        .flatMapMerge { it.consumeAsFlow() }.collect { message ->
          when (message) {
            is ActorMessage<*> -> message(message)
            is Payload<*> -> payload(message as Payload<T>, this@ActorData)
          }
        }
    }
  }


  fun <A : Any> addChild(child: ActorRef<A>) {
    children[child.id] = child
  }

  fun print() = "{$id: Parent: ${parent?.id}, Children: ${children.keys}}"

  fun ref() = lazy { ActorRef(clazz, inboundActorMessages, inboundBusinessChannel, id, state) }.value

  private fun determineMessageSource(actorRef: ActorRef<*>?): MessageSource =
    when {
      actorRef == parent -> Parent
      children.contains(actorRef?.id) -> Child
      else -> Unknown
    }

  suspend fun finished() {
    state = ActorState.FINISHED
    if (parent != null) parent!!.sendMessage(Finished<Any>(ref()))
    this.close()
  }


  fun close() {
    inboundBusinessChannel.close()
    inboundActorMessages.close()
  }



  suspend fun handleActorMessage(actorMessage: ActorMessage<*>) {
    val source = actorMessage.source
    val sourceType = determineMessageSource(source)

    when (actorMessage) {
      is Finished -> when (sourceType) {
        Parent -> error("Received FINISHED from parent")
        Child -> {
          children[source.id] = source
          if (children.values.none { it.state != ActorState.FINISHED }) {
            finished()
          }
        }
        Unknown -> error("Received FINIHSED FROM unknown")
      }
      is PoisonPill -> this.close()
    }
  }

  suspend inline fun <reified A : Any> createChild(noinline payload: suspend (Payload<A>, ActorData<A>) -> Unit): ActorData<A> {
    val child = createActor(payload)
    child.parent = this.ref()
    addChild(child.ref())
    return child
  }

}

suspend inline fun <reified T : Any> CoroutineScope.createActor(
  noinline payload: suspend (Payload<T>, ActorData<T>) -> Unit
): ActorData<T> {

  val id = SYSTEM.getId()
  val actorData = ActorData<T>(
    T::class,
    Channel(),
    Channel(),
    null,
    mutableMapOf(),
    this.coroutineContext,
    id
  )

  actorData.listener(
    payload,
    actorData::handleActorMessage
  )
  return actorData
}

sealed class Message<T>
interface ActorMessageData {
  val source: ActorRef<*>
}

sealed class ActorMessage<T> : Message<T>(), ActorMessageData
data class Finished<T>(override val source: ActorRef<*>) : ActorMessage<T>()
data class PoisonPill<T>(override val source: ActorRef<*>) : ActorMessage<T>()
data class Payload<T>(val value: T) : Message<T>()
