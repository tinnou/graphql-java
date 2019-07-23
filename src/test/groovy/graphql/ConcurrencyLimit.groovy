package graphql


import graphql.schema.DataFetcher
import graphql.schema.DataFetchingEnvironment
import graphql.schema.idl.RuntimeWiring
import graphql.schema.idl.TypeRuntimeWiring
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.Flowable
import io.reactivex.functions.Consumer
import io.reactivex.schedulers.TestScheduler
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

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

    def schema = """
            type Query {
                posts(limit: Int!): [Post]
            }
            
            type Post {
                id: ID!
                author: String
            }
        """

    def executor = Executors.newFixedThreadPool(100)

    def hi() {
        def postsDf = new DataFetcher() {
            @Override
            Object get(DataFetchingEnvironment env) {
                def times = env.getArgument("limit") as Integer
                def posts = []
                for (def it = 1; it <= times; it++) {
                    posts.add([id: "Post$it".toString(), authorId: "$it"])
                }
                return posts
            }
        }

        def authorDf = new DataFetcher() {
            @Override
            CompletableFuture<Object> get(DataFetchingEnvironment env) {
                CompletableFuture.supplyAsync( {
                    System.out.println(Thread.currentThread().getName() + " Resolving author for ${env.source.id}")
                    System.out.println(Thread.currentThread().getName() + " Sleeping...")
                    Thread.sleep(1000)
                    def authorId = env.source["authorId"]
                    return "author$authorId"
                }, executor)
            }
        }

        def runtimeWiring = RuntimeWiring.newRuntimeWiring()
                .type(TypeRuntimeWiring.newTypeWiring("Query")
                                .dataFetcher("posts", postsDf)
                                .build())
                .type(TypeRuntimeWiring.newTypeWiring("Post")
                        .dataFetcher("author", authorDf)
                        .build())
                .build()
        def schema = TestUtil.schema(schema, runtimeWiring)

        def graphql = GraphQL.newGraphQL(schema).build()

        when:
        def times = 10
        def input = ExecutionInput.newExecutionInput()
                .query("""
                        query {
                            posts(limit: $times) {
                              id 
                              author
                            }
                        }
                        """)
                .build()
        def executionResult = graphql.execute(input)
        def authorIds = []
        for (def it = 1; it <= times; it++) {
            authorIds.add("author$it")
        }

        then:
        executionResult.errors.isEmpty()
        executionResult.data.posts.author == authorIds
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
        root.left = createSubTree(root, levels)
        root.right = createSubTree(root, levels)
        return root
    }

    TreeNode createSubTree(TreeNode parent, int levels) {
        if (parent.level == levels) {
            return null
        }

        def current = new TreeNode(value: parent.value.toInteger() + 1, level: parent.level + 1, parent: parent)
        current.left = createSubTree(current, levels)
        current.right = createSubTree(current, levels)
        return current
    }

    static class TreeNode {
        TreeNode left
        TreeNode right
        TreeNode parent

        String value
        int level
        Single<String> fetchedValue
        String actualFetchedValue


        // variables needed to print the tree like a tree
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
            log.info("fetched Value {}", "F" + current.value )
            return "F" + current.value
        })
    }




    /************ Actual functions that print the tree like a tree ********************/
    static void drawTree(TreeNode root)
    {

        System.out.println("\n\nLevel order traversal of tree:");
        int depth = depth(root);
        setLevels (root, 0);

        int[] depthChildCount = new int [depth+1];


        LinkedList<TreeNode> q = new  LinkedList<TreeNode> ();
        q.add(root.left);
        q.add(root.right);

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
            q.add(ele.left);
            q.add(ele.right);
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