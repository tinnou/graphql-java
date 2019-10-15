package graphql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import graphql.execution.instrumentation.ChainedInstrumentation
import graphql.execution.instrumentation.Instrumentation
import graphql.execution.instrumentation.dataloader.DataLoaderDispatcherInstrumentation
import graphql.schema.DataFetcher
import graphql.schema.GraphQLSchema
import graphql.schema.idl.RuntimeWiring
import graphql.schema.idl.SchemaGenerator
import graphql.schema.idl.SchemaParser
import graphql.schema.idl.TypeDefinitionRegistry
import org.dataloader.BatchLoaderWithContext
import org.dataloader.DataLoader
import org.dataloader.DataLoaderOptions
import org.dataloader.DataLoaderRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring

class ConcurrencyTest extends Specification {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class)
    private static final ObjectMapper mapper = new ObjectMapper()

    static {
        mapper.enable(SerializationFeature.INDENT_OUTPUT)
    }

    def "a simple batch loading test with concurrency limitation"() {

        def loaders = new DataFetchersAndBatchLoaders()
        // the batch loader is async
        BatchLoaderWithContext<String, Object> postAuthorBL = loaders.postAuthorBL

        DataFetcher postsDF = { env ->
            return [[id:'p1', title:'Post1', authorId: 'a1'],
                    [id:'p2', title:'Post2', authorId: 'a2'],
                    [id:'p3', title:'Post3', authorId: 'a3'],
                    [id:'p4', title:'Post4', authorId: 'a4'],
                    [id:'p5', title:'Post5', authorId: 'a5'],
                    [id:'p6', title:'Post6', authorId: 'a6'],
                    [id:'p7', title:'Post7', authorId: 'a7'],
                    [id:'p8', title:'Post8', authorId: 'a8'],
                    [id:'p9', title:'Post9', authorId: 'a9'],
                    [id:'p10', title:'Post10', authorId: 'a10']]
        }

        DataFetcher authorDF = { env ->
            def dlRegistry = env.context.dlRegistry as DataLoaderRegistry
            return dlRegistry.getDataLoader('post.author').load(env.source.authorId, env.getContext())
        }

        RuntimeWiring runtimeWiring = RuntimeWiring.newRuntimeWiring()
                .type(newTypeWiring("Query").dataFetcher("posts", postsDF))
                .type(newTypeWiring("Post").dataFetcher("author", authorDF))
//                .type(newTypeWiring("Author").dataFetcher("posts", authorPostsDF))
                .build()

        String sdl = '''
                    schema {
                        query: Query
                    }
                    
                    type Query {
                        posts: [Post]
                    }
                    
                    type Author {
                        id: ID!
                        firstName: String
                        lastName: String
                        posts: [Post]
                    }
                    
                    type Post {
                        id: ID!
                        title: String
                        votes: Int
                        author: Author
                    }'''

        TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(sdl)

        GraphQLSchema graphQLSchema = new SchemaGenerator().makeExecutableSchema(typeRegistry, runtimeWiring)

        when:
        // we dont cache to ensure this DataLoader / BatchLoader gets some real work!
        DataLoaderOptions noCaching = DataLoaderOptions.newOptions().setCachingEnabled(false).setBatchingEnabled(false)
        DataLoader<String, Object> postAuthorDL = DataLoader.newDataLoader(postAuthorBL, noCaching);

        DataLoaderRegistry dataLoaderRegistry = new DataLoaderRegistry()
        dataLoaderRegistry.register("post.author", postAuthorDL)

        final List<Instrumentation> instrumentations = new ArrayList<>()
        instrumentations.add(new DataLoaderDispatcherInstrumentation(dataLoaderRegistry))

        GraphQL graphQL = GraphQL.newGraphQL(graphQLSchema)
                .instrumentation(new ChainedInstrumentation(instrumentations))
                .build()


        String query = '''
            query {
                posts {
                    id
                    title
                    author {
                        id
                        firstName
                        lastName
                        posts {
                            id
                        }
                    }
                }
            }
        '''

        BatchLoadingTaskProcessor batchLoadingTaskProcessor = new BatchLoadingTaskProcessor(Executors.newFixedThreadPool(15), postAuthorBL, 3)
        batchLoadingTaskProcessor.start()

        ExecutionInput ei = ExecutionInput.newExecutionInput()
                .context([dlRegistry: dataLoaderRegistry, channel: batchLoadingTaskProcessor.channel])
                .query(query)
                .build()

        then:
        CompletableFuture<ExecutionResult> cfResult = graphQL.executeAsync(ei)
        def executionResult = cfResult.get()
        batchLoadingTaskProcessor.shutdown()


        def data = executionResult.getData()
        log.info("Data {}", mapper.writeValueAsString(data))

        executionResult.errors.isEmpty()

    }

    public static void randomSnoozeMs(int minMs, int maxMs) {
        Duration duration = Duration.of(getRandomNumberInRange(minMs, maxMs), ChronoUnit.MILLIS);
        try {
            log.info("Sleep duration "+ duration);
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static int getRandomNumberInRange(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }
}
