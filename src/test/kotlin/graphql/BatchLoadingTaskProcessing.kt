package graphql

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import kotlin.coroutines.EmptyCoroutineContext


/**
 * This Processor polls tasks off a Coroutine Channel.
 * It launches initially a set of coroutines based on the desired {@code maxConcurrency}
 * Each coroutine processes the tasks coming from the channel.
 *
 */
class BatchLoadingTaskProcessor @JvmOverloads constructor(executor: Executor,
                                                          private val batchLoader: PostAuthorBL,
                                                          private val maxConcurrency: Int = 5) {

    private val coroutineDispatcher: CoroutineDispatcher = executor.asCoroutineDispatcher()
    private val coroutineScope = CoroutineScope(EmptyCoroutineContext) + coroutineDispatcher
    val channel = Channel<BatchLoadingTaskParameters>(Channel.UNLIMITED)


    fun start() {
        //TODO as an optimization we could increase the number of coroutines processing tasks
        // as we go, instead of all at once.
        for (i in 1..maxConcurrency) {
            coroutineScope.launch(CoroutineName("BatchLoader-$i")) {
                processMessage(channel)
            }
        }
    }

    fun shutdown() {
        channel.close()
    }

    private suspend fun processMessage(channel: ReceiveChannel<BatchLoadingTaskParameters>) {
        // the channel iterator here suspends until there is a message in the channel to be polled
        for (message in channel) {
            log.info("Read message, invoking batchLoading task")

            val values = batchLoader.loadNow(message.keys)
            // complete the future when the task is over
            message.results.complete(values)
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(BatchLoadingTaskProcessor::class.java)
    }
}


/**
 * Channel message for batchLoading tasks.
 */
data class BatchLoadingTaskParameters(
        val keys: List<String>,
        val fieldName: String?,
        val results: CompletableFuture<MutableList<Any>>)
