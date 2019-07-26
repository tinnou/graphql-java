package graphql

import graphql.execution.Async
import graphql.execution.instrumentation.SimpleInstrumentation
import graphql.schema.DataFetcher
import graphql.schema.DataFetchingEnvironment
import graphql.schema.idl.RuntimeWiring
import graphql.schema.idl.TypeRuntimeWiring
import hu.akarnokd.rxjava2.operators.ExpandStrategy
import hu.akarnokd.rxjava2.operators.FlowableTransformers
import io.reactivex.Flowable
import io.reactivex.Observable
import io.reactivex.Scheduler
import io.reactivex.Single
import io.reactivex.internal.operators.flowable.FlowableSingle
import io.reactivex.schedulers.Schedulers
import io.reactivex.schedulers.TestScheduler
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

class ConcurrencyLimitSimple extends Specification {

    TestScheduler testScheduler = new TestScheduler()

    /**
     * A query can result in N field resolution.
     * N is not known before executing the query.
     * At each level, the strategy "discovers" the fields
     * to resolver for the current level.
     *
     * On a single server we would like to cap:
     * - A: the total number of executed queries - OK (Max Request Handler)
     * - B: the number of incoming queries for a given account - OK (Coral Throttle)
     * - C: the total number of fields being executed - X
     * - D: the number of fields being executed for a given account - X
     *
     * C: This would be relatively simple, we just keep an Atomic counter of
     * all the fields being currently executed in the JVM and drop new incoming
     * queries.
     *
     * D: This one is similar but we can't really do anything about it, because there
     * is not any thread-pool that we can limit the parallelism for a single account.
     *
     *
     * I can implement different levels of concurrent field resolution:
     *
     *  ++ Limiting field execution concurrency for a query using Java
     *      requires the use of thread-pools. A Thread pool would have to
     *      instantiated per request which will put pressure on GC and memory.
     *
     *
     */
    private final static Logger log = LoggerFactory.getLogger(ConcurrencyLimitSimple.class);

    def rand = ThreadLocalRandom.current()

    def executor = Executors.newFixedThreadPool(100, new BasicThreadFactory.Builder()
            .namingPattern("datafetcher-get-pool-%s").build())


//    def "rxjava : defer() makes the completable future call a cold observable"() {
//
//        TestScheduler testScheduler = new TestScheduler()
//        Single<String> singleOfFuture = Single.defer( {
//            Single.fromFuture(getValueFuture()).observeOn(testScheduler)
//        }).observeOn(testScheduler)
//
//        def returnedValue
//        when:
//        log.info("About to subscribe")
//        singleOfFuture.subscribe({ s ->
//            log.info(" In SingleObserver")
//            log.info(" Printing value: "  + s)
//            returnedValue = s
//        } as Consumer)
//        log.info("About to trigger actions")
//        testScheduler.triggerActions()
//
//        then:
//        returnedValue == "OK"
//    }
//
//    private CompletableFuture<String> getValueFuture() {
//        CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync({
//            log.info(" In completableFuture, Sleeping...")
//            Thread.sleep(1000)
//            return "OK"
//        })
//        completableFuture
//    }

    def "example with simple iterable, limit concurrency in flat map using Observable"() {

        List<Integer> l = (1..100).toList()

        def exec = Executors.newFixedThreadPool(4)
        when:
        // Below works as well as flowable but is a little less readable.
        Observable<Integer> obs = Observable.fromIterable(l)
        Observable<Integer> obs2 = Observable.fromIterable(l)
        def otherObs = obs
                .flatMap({ i ->
                    Observable.defer({ Observable.fromFuture(fetchValueFuture(i), Schedulers.from(exec)) })
                }, false, 3)
                .doAfterNext({ string -> log.info("string fetchedValue {}", string) } )

        def otherObs2 = obs2
                .flatMap({ i ->
                    Observable.defer({ Observable.fromFuture(fetchValueFuture(i), Schedulers.from(exec)) })

                }, false, 3)
                .doAfterNext({ string -> log.info("string fetchedValue 2 {}", string) } )

        log.info("About to sleep once")
        TimeUnit.SECONDS.sleep(2)
        log.info("About to subscribe")
        otherObs.subscribe()
        otherObs2.subscribe()
        Thread.sleep(100_000)

        then:
        true
    }


    def "example with simple iterable, limit concurrency in flat map using Flowable"() {

        List<Integer> l = (1..100).toList()

        def exec = Executors.newFixedThreadPool(4)

        when:
        Flowable<Integer> obs = Flowable.fromIterable(l)
        Flowable<Integer> obs2 = Flowable.fromIterable(l)
        def otherObs = obs
                .parallel(3)
                .flatMap({ i -> FlowableSingle.fromFuture(fetchValueFuture(i), Schedulers.from(exec)) }, false,1)
                .doAfterNext({ string -> log.info("string fetchedValue {}", string) } )
                .sequential()

        def otherObs2 = obs2
                .parallel(3)
                .flatMap({ i -> FlowableSingle.fromFuture(fetchValueFuture(i), Schedulers.from(exec)) }, false,1)
                .doAfterNext({ string -> log.info("string fetchedValue 2 {}", string) } )
                .sequential()

        log.info("About to sleep once")
        TimeUnit.SECONDS.sleep(2)
        log.info("About to subscribe")
        otherObs.subscribe()
        otherObs2.subscribe()
        Thread.sleep(100_000)

        then:
        true
    }


    def "example with recursive data structure, limit concurrency in flat map using Flowable"() {

        List<Integer> l = (1..100).toList()

        def exec = Executors.newFixedThreadPool(4)

        when:
        Flowable<String> obs = Flowable.fromIterable(l).map { it.toString() }
        def stringFanout = obs.flatMap({ fanOut(obs, exec) }, false, 3)


        log.info("About to sleep once")
        TimeUnit.SECONDS.sleep(2)
        log.info("About to subscribe")
        stringFanout.subscribe()
        Thread.sleep(100_000)

        then:
        true
    }

    AtomicInteger parallelInvocations = new AtomicInteger(0)

    Flowable<String> fanOut(Flowable<String> obs, Executor exec) {
        return obs
                .parallel(3)
                .flatMap({ i ->
                    FlowableSingle.fromFuture(fetchValueFuture(i), Schedulers.from(exec))
                }, false, 1)
                .doAfterNext({ string -> log.info("string fetchedValue {}", string) } )
                .sequential()
    }


    Flowable<String> fanOut(String str, Executor exec) {
        return Flowable.just(str)
                .flatMap({ str1 ->
                    List<Integer> l = (1..10).toList()
                    return Flowable.fromIterable(l).flatMap({ i ->
                        return FlowableSingle.fromFuture(fetchValueFuture(i.toString()), Schedulers.from(exec))
                                .flatMap( { fanOut(str, exec) }, false, 1)
                    }, false, 1)
                }, false, 1)
                .doAfterNext({ string -> log.info("string fetchedValue {}", string) } )
    }


    def "example with recursive data, limit concurrency in flat map using expand()"() {

        List<Map<String, Object>> l = (1..100).toList().stream()
                                    .map { [id: it, comments:[1,2,3,4]]}
                                    .collect(Collectors.toList())

        def exec = Executors.newFixedThreadPool(4, new BasicThreadFactory.Builder()
                .namingPattern("fanout-pool-%s").build())


        def expected = [[id: 1, comments:[1, 2, 3, 4, 5]]]
                .stream()
                .flatMap({
                    def its = it
                    its.comments.stream().flatMap {
                        def childComments = its.comments.drop(1)

                        childComments.stream().map {
                            [id: "F" + it, comments: childComments]
                        }
                    }
                })
                .toArray() as Map<String, Object>[]

        log.info("expected $expected")


        when:
        Flowable.just([id: 1, comments:[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]])
                .compose(FlowableTransformers.expand({ post ->
                    if (post.comments.isEmpty()) {
                        return Flowable.empty()
                    }

                    // drop a comment at each level so it can end the recursion.
                    def childComments = post.comments.drop(1)

                    def commentsFlowable = Flowable.fromIterable(post.comments)
                                    .flatMap({ cId -> Flowable.defer { Flowable.fromFuture(fetchValueFuture(cId.toString()), Schedulers.from(exec)) }  }, false, 8)
                                    .map { [id: it, comments:childComments] }

                    return commentsFlowable
                }, ExpandStrategy.BREADTH_FIRST))
                .subscribe{ log.info("p $it")};
        Thread.sleep(10_000)
        then:
        true
    }

    CompletableFuture<String> fetchValueFuture(String current) {
        return CompletableFuture.supplyAsync({
            def currentInvocationsCount = parallelInvocations.incrementAndGet()
            //assert currentInvocationsCount <= 6
            log.info("Invocations: " + currentInvocationsCount)
            log.info("Sleeping...")
            Thread.sleep(randInt(1000,1001))
            def invo = parallelInvocations.decrementAndGet()
            //log.info("Invocations: " + invo)
            return "F" + current
        }, executor)
    }

    int randInt(int min, int max) {
        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min

        return randomNum
    }

}