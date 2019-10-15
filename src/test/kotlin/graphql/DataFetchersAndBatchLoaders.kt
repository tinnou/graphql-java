package graphql

import graphql.ConcurrencyTest.randomSnoozeMs
import kotlinx.coroutines.channels.SendChannel
import org.dataloader.BatchLoaderEnvironment
import org.dataloader.BatchLoaderWithContext
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.stream.Collectors

object DF {
    val log = LoggerFactory.getLogger(DataFetchersAndBatchLoaders::class.java)
}

class DataFetchersAndBatchLoaders{

    // the batch loader is async
    val postAuthorBL : PostAuthorBL = PostAuthorBL()
}


class PostAuthorBL: BatchLoaderWithContext<String, Any> {

    override fun load(keys: List<String>, environment: BatchLoaderEnvironment): CompletionStage<MutableList<Any>> {
        // get the query context
        val context = environment.keyContextsList.stream().findFirst().get() as Map<String, Any>
        val channel = context["channel"] as SendChannel<BatchLoadingTaskParameters>

        // results will be completed once any coroutine is done handling the task
        val results = CompletableFuture<MutableList<Any>>()
        channel.offer(BatchLoadingTaskParameters(keys = keys, results = results, fieldName = null))
        return results
    }

    fun loadNow(keys: List<String>): MutableList<Any>  {
        //
        // this batch load is going to simulate some delay -e g work being done
        DF.log.info("snoozing in batch loading")
        randomSnoozeMs(4999, 5000)
        val authors : MutableList<Any> = keys.stream()
                .map { key ->
                    mapOf("id" to key, "firstName" to "firstName-$key", "lastName" to "lastName-$key")
                }.collect(Collectors.toList())
        return authors
    }
}
