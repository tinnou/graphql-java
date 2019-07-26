package graphql

import graphql.execution.Async
import graphql.execution.instrumentation.SimpleInstrumentation
import graphql.schema.DataFetcher
import graphql.schema.DataFetchingEnvironment
import graphql.schema.idl.RuntimeWiring
import graphql.schema.idl.TypeRuntimeWiring
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.Flowable
import io.reactivex.functions.Consumer
import io.reactivex.internal.operators.flowable.FlowableSingle
import io.reactivex.schedulers.Schedulers
import io.reactivex.schedulers.TestScheduler
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

class ConcurrencyLimit extends Specification {

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
    private final static Logger log = LoggerFactory.getLogger(ConcurrencyLimit.class);

    def rand = ThreadLocalRandom.current()

    def schema = """
            type Query {
                allPosts(limit: Int!): [Post]
            }
            
            type Post {
                id: ID!
                comments(limit: Int!): [Comment]
            }
            
            type Comment {
                id: ID!
                posts(limit: Int!): [Post]
            }
        """

    def executor = Executors.newFixedThreadPool(100, new BasicThreadFactory.Builder()
            .namingPattern("datafetcher-get-pool-%s").build())

    def "test list of lists"() {
        def allPostsDf = new DataFetcher() {
            @Override
            Object get(DataFetchingEnvironment env) {
                def times = env.getArgument("limit") as Integer
                def level = 1
                def posts = []
                log.info("Resolving Query.posts level:$level")
                for (def it = 1; it <= times; it++) {
                    posts.add([id: "Post$it".toString(), authorId: "$it", level: level])
                }
                return posts
            }
        }

        def postsDf = new DataFetcher() {
            @Override
            Object get(DataFetchingEnvironment env) {
                def times = env.getArgument("limit") as Integer
                def level =  (env.getSource().level as Integer) + 1
                def commentId = env.source.id == null ? "" : env.source.id
                def posts = []
                log.info("Resolving Comment.posts $level")
                for (def it = 1; it <= times; it++) {
                    posts.add([id: "${commentId}-Post$it".toString(), authorId: "$it", level: level])
                }
                return posts
            }
        }

        def commentsDf = new DataFetcher() {
            @Override
            Object get(DataFetchingEnvironment env) {
                def times = env.getArgument("limit") as Integer
                def level =  (env.source.level as Integer) + 1
                def postId = env.source.id
                def posts = []
                log.info("Resolving Post.comments level:$level")
                for (def it = 1; it <= times; it++) {
                    posts.add([id: "${postId}-Comment$it".toString(), authorId: "$it", level: level])
                }
                return posts
            }
        }

        def authorDf = new DataFetcher() {
            @Override
            CompletableFuture<Object> get(DataFetchingEnvironment env) {
                CompletableFuture.supplyAsync( {
                    log.info(" Resolving author for ${env.source.id}")
                    log.info(" Sleeping...")
                    Thread.sleep(1000)
                    def authorId = env.source["authorId"]
                    return "author$authorId"
                }, executor)
            }
        }

        def idDf = new DataFetcher() {
            @Override
            CompletableFuture<Object> get(DataFetchingEnvironment env) {
                CompletableFuture.supplyAsync( {
                    log.info(" Resolving id for ${env.source.id} level:$env.source.level")
                    log.info(" Sleeping...")
                    Thread.sleep(1000)
                    return env.source.id
                }, executor)
            }
        }

        def runtimeWiring = RuntimeWiring.newRuntimeWiring()
                .type(TypeRuntimeWiring.newTypeWiring("Query")
                                .dataFetcher("allPosts", allPostsDf)
                                .build())
                .type(TypeRuntimeWiring.newTypeWiring("Post")
                        .dataFetcher("comments", commentsDf)
                        .build())
                .type(TypeRuntimeWiring.newTypeWiring("Post")
                        .dataFetcher("author", authorDf)
                        .build())
                .type(TypeRuntimeWiring.newTypeWiring("Post")
                        .dataFetcher("id", idDf)
                        .build())
                .type(TypeRuntimeWiring.newTypeWiring("Comment")
                        .dataFetcher("id", idDf)
                        .build())
                .type(TypeRuntimeWiring.newTypeWiring("Comment")
                        .dataFetcher("posts", postsDf)
                        .build())
                .build()
        def schema = TestUtil.schema(schema, runtimeWiring)

        def graphql = GraphQL.newGraphQL(schema)
                .instrumentation(SimpleInstrumentation.INSTANCE).build()

        when:
        def times = 10
        def input = ExecutionInput.newExecutionInput()
                .query("""
                        query {
                            allPosts(limit: $times) {
                              id 
                              comments(limit: $times) {
                                  id
                                  posts(limit: $times) {
                                    id
                                  }
                              }
                            }
                        }
                        """)
                .build()
        def executionResultAsync = graphql.executeAsync(input)
//        def executionResultAsync2 = graphql.executeAsync(input)
//        def executionResultAsync3 = graphql.executeAsync(input)

        def authorIds = []
        for (def it = 1; it <= times; it++) {
            authorIds.add("author$it")
        }
        def future = Async.each(Arrays.asList(executionResultAsync)).get()
//        def future = Async.each(Arrays.asList(executionResultAsync, executionResultAsync2, executionResultAsync3)).get()
        def executionResult = future.get(0)
//        def executionResult2 = future.get(1)
//        def executionResult3 = future.get(2)


        then:
        executionResult.errors.isEmpty()
        executionResult.data.posts.author == authorIds
//        executionResult2.errors.isEmpty()
//        executionResult2.data.posts.author == authorIds
//        executionResult3.errors.isEmpty()
//        executionResult3.data.posts.author == authorIds
    }

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


    def "backpressure example with simple iterable"() {

        List<Integer> l = (1..100).toList()

        def exec = Executors.newFixedThreadPool(4)
        when:
        // Below works as well as flowable but is a little less readable.
//        Observable<Integer> obs = Observable.fromIterable(l)
//        Observable<Integer> obs2 = Observable.fromIterable(l)
//        def otherObs = obs
//                .flatMap({ i ->
//                    Observable.defer({ Observable.fromFuture(fetchValueFuture(i), Schedulers.from(exec)) })
//                }, false, 3)
//                .doAfterNext({ string -> log.info("string fetchedValue {}", string) } )
//
//        def otherObs2 = obs2
//                .flatMap({ i ->
//                    Observable.defer({ Observable.fromFuture(fetchValueFuture(i), Schedulers.from(exec)) })
//
//                }, false, 3)
//                .doAfterNext({ string -> log.info("string fetchedValue 2 {}", string) } )

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

    def "back pressure example using a tree"() {

        def tree = createTree()

        drawTree(tree)

        when:
        traverseAndFetchValue(tree)

        then: "with all the acutal fetched values"
        drawTree(tree)
    }

    TreeNode createTree(int levels = 5, int initialValue = 1) {
        def root = new TreeNode(value: initialValue, level: 1, parent: null)
        root.children = createSubTree(root, levels)
        return root
    }

    List<TreeNode> createSubTree(TreeNode parent, int levels) {
        if (parent.level == levels) {
            return null
        }

        List<TreeNode> children = []
        for (def i = parent.level + 1; i<=levels; i++) {
            def current = new TreeNode(value: parent.value.toInteger() + 1, level: parent.level + 1, parent: parent)
            current.children = createSubTree(current, levels)
            children.add(current)
        }

        return children
    }

    static class TreeNode {
        String value
        TreeNode parent
        List<TreeNode> children

        Single<String> fetchedValue
        String actualFetchedValue


        // variables needed to print the tree like a tree
        int level
        int depth=0
        int drawPos=0

//        @Override
//        public String toString() {
//            return "Node{" +
//                    "left=" + left +
//                    ", right=" + right +
//                    ", value='" + value + '\'' +
//                    ", level=" + level +
//                    '} \n';
//        }
    }

    void traverseAndFetchValue(TreeNode root) {

        Observable<TreeNode> fetchedValue = fetchValue(root)

        fetchedValue.subscribe({next ->
            log.info("node: value: {}, level: {}", next.actualFetchedValue, next.level)
        }, {error -> throw error}, {
            log.info("Completed")
        })
    }


    Observable<TreeNode> fetchValue(TreeNode current) {

        if (current == null) {
            return null
        }

        Observable<TreeNode> obs = fetchOne(current)

        if (current.left != null) {
            obs = obs.mergeWith(fetchOne(current.left).flatMap({ it -> fetchValue(it) }))
        }

        if (current.right != null) {
            obs = obs.mergeWith(fetchOne(current.right).flatMap({ it -> fetchValue(it) }))
        }

        return obs
    }


    Observable<TreeNode> fetchOne(TreeNode current) {
        return Observable.defer {
            Observable.fromFuture(fetchValueFuture(current)
                    .thenApply({ value ->
                        log.info("fetched Value c {}", value)
                        current.actualFetchedValue = value
                        return current
                    }))
        }
    }

    CompletableFuture<String> fetchValueFuture(TreeNode current) {
        return CompletableFuture.supplyAsync({
            Thread.sleep(100)
            return "F" + current.value
        })
    }


    CompletableFuture<String> fetchValueFuture(Integer current) {
        return CompletableFuture.supplyAsync({
            log.info("Sleeping...")
            Thread.sleep(randInt(1000,1001))
            return "F" + current
        }, executor)
    }

    int randInt(int min, int max) {
        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }

    /************ Actual functions that print the tree like a tree ********************/
    static void drawTree(TreeNode root)
    {

        System.out.println("\n\nLevel order traversal of tree:");
        int depth = depth(root);
        setLevels (root, 0);

        int[] depthChildCount = new int [depth+1];


        LinkedList<TreeNode> q = new  LinkedList<TreeNode> ();
        q.addAll(root.children);

        // draw root first
        root.drawPos = (int)Math.pow(2, depth-1)*H_SPREAD;
        currDrawLevel = root.level;
        currSpaceCount = root.drawPos;
        System.out.print(getSpace(root.drawPos) + root.value);

        while (!q.isEmpty())
        {
            TreeNode ele = q.pollFirst();
            drawElement (ele, depthChildCount, depth, q);
            if (ele == null)
                continue;
            q.addAll(root.children)
        }
        System.out.println();
    }

    static int currDrawLevel  = -1;
    static int currSpaceCount = -1;
    static final int H_SPREAD = 3;
    static void drawElement(TreeNode ele, int[] depthChildCount, int depth, LinkedList<TreeNode> q)
    {
        if (ele == null)
            return;

        if (ele.level != currDrawLevel)
        {
            currDrawLevel = ele.level;
            currSpaceCount = 0;
            System.out.println();
            for (int i=0; i<(depth-ele.level+1); i++)
            {
                int drawn = 0;
                if (ele.parent.left != null)
                {
                    drawn = ele.parent.drawPos - 2*i - 2;
                    System.out.print(getSpace(drawn) + "/");
                }
                if (ele.parent.right != null)
                {
                    int drawn2 = ele.parent.drawPos + 2*i + 2;
                    System.out.print(getSpace(drawn2 - drawn) + "\\");
                    drawn = drawn2;
                }

                TreeNode doneParent = ele.parent;
                for (TreeNode sibling: q)
                {
                    if (sibling == null)
                        continue;
                    if (sibling.parent == doneParent)
                        continue;
                    doneParent = sibling.parent;
                    if (sibling.parent.left != null)
                    {
                        int drawn2 = sibling.parent.drawPos - 2*i - 2;
                        System.out.print(getSpace(drawn2-drawn-1) + "/");
                        drawn = drawn2;
                    }

                    if (sibling.parent.right != null)
                    {
                        int drawn2 = sibling.parent.drawPos + 2*i + 2;
                        System.out.print(getSpace(drawn2-drawn-1) + "\\");
                        drawn = drawn2;
                    }
                }
                System.out.println();
            }
        }
        int offset=0;
        int numDigits = (int)Math.ceil(Math.log10(ele.value.toInteger()));
        if (ele.actualFetchedValue != null) {
            numDigits = (numDigits * 2) + 2
        }
        if (ele.parent.left == ele)
        {
            ele.drawPos = ele.parent.drawPos - H_SPREAD*(depth-currDrawLevel+1);
            //offset = 2;
            offset += numDigits/2;
        }
        else
        {
            ele.drawPos = ele.parent.drawPos + H_SPREAD*(depth-currDrawLevel+1);
            //offset = -2;
            offset -= numDigits;
        }

        def value = ele.actualFetchedValue != null ? ele.value + "-" + ele.actualFetchedValue : ele.value
        System.out.print (getSpace(ele.drawPos - currSpaceCount + offset) + value);
        currSpaceCount = ele.drawPos + numDigits/2;
    }


    // Utility functions

    public static int depth (TreeNode n)
    {
        if (n == null)
            return 0;
        n.depth = 1 + Math.max(depth(n.left), depth(n.right));
        return n.depth;
    }


    public static int countNodes (TreeNode n)
    {
        if (n == null)
            return 0;
        return 1 + countNodes(n.left) + countNodes(n.right);
    }

    static void setLevels (TreeNode r, int level)
    {
        if (r == null)
            return;
        r.level = level;
        setLevels (r.left, level+1);
        setLevels (r.right, level+1);
    }

    static String getSpace (int i)
    {
        String s = "";
        while (i-- > 0)
            s += " ";
        return s;
    }
}