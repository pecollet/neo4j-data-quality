package neo4j.dq;


import org.junit.*;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;


import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class DQTest {
    private static final String TEST_DATA = "CREATE " +
            "(a:Node{name:'A'}), " +
            "(b:Node{name:'B'}), " +
            "(c:Node{name:'C'}), " +
            "(a)-[:MY_NON_DEFAULT_IMPACT_RELATION]->(b), " +
            "(a)-[:WHATEVER]->(c) " ;
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, singletonList("neo4j.dq.*"));

    @Before
    public void setUp() throws Exception {
        registerProcedure(db, DQ.class);
       // registerProcedure(db, apoc.generate.Generate.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }


    //test for each propagation rule
    @Test
    public void testDQ_createFlag() throws Exception {

        db.executeTransactionally(TEST_DATA);
        // custom relation, either way
        testResult(db, "MATCH (a:Node) WHERE a.name='A' " +
                        "CALL neo4j.dq.createFlag(a, 'BadName', 'nodes should not be called A') yield flag " +
                        "RETURN  flag" , null,
                r -> assertDqResult(r, "nodes should not be called A") );

    }



    private void assertDqResult(Result r, String expectedDescription) {
        Node flag;
        for (Map<String, Object> map : Iterators.asList(r)) {
            flag = (Node) map.get("flag");
            String description = flag.getProperty("description").toString();
            assertEquals(expectedDescription, description);
        }
    }


    public static void testResult(GraphDatabaseService db, String call, Map<String,Object> params, Consumer<Result> resultConsumer) {
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> p = (params == null) ? Collections.emptyMap() : params;
            Result result = tx.execute(call, p);
            resultConsumer.accept(result);
            tx.commit();
        } catch (RuntimeException e) {
            throw e;
        }
    }
    public static void registerProcedure(GraphDatabaseService db, Class<?>...procedures) {
        GlobalProcedures globalProcedures = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(GlobalProcedures.class);
        for (Class<?> procedure : procedures) {
            try {
                globalProcedures.registerProcedure(procedure, true);
                globalProcedures.registerFunction(procedure, true);
                globalProcedures.registerAggregationFunction(procedure, true);
            } catch (KernelException e) {
                throw new RuntimeException("while registering " + procedure, e);
            }
        }
    }
}
