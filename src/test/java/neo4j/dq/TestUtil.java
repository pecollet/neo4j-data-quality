package neo4j.dq;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class TestUtil {
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

    private static <T> ResourceIterator<T> iteratorSingleColumn(Result result) {
        return result.columnAs(Iterables.single(result.columns()));
    }
    public static <T> T singleResultFirstColumn(GraphDatabaseService db, String cypher, Map<String,Object> params) {
        return db.executeTransactionally(cypher, params, result -> Iterators.singleOrNull(iteratorSingleColumn(result)));
    }

    public static <T> List<T> firstColumn(GraphDatabaseService db, String cypher, Map<String,Object> params) {
        return db.executeTransactionally(cypher , params, result -> Iterators.asList(iteratorSingleColumn(result)));
    }
}
