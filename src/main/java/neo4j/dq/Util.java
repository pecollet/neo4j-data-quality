package neo4j.dq;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Function;

public class Util {

    public static Node node(Transaction tx, Object id) {
        if (id instanceof Node) return (Node)id;
        if (id instanceof Number) return tx.getNodeById(((Number)id).longValue());
        throw new RuntimeException("Can't convert "+id.getClass()+" to a Node");
    }

    public static boolean isNullOrEmpty(String s) {
        return s==null || s.trim().length()==0;
    }

    public static List convertToList(Object list) {
        if (list == null) return null;
        else if (list instanceof List) return (List) list;
        else if (list instanceof Collection) return new ArrayList((Collection)list);
        else if (list instanceof Iterable) return Iterators.addToCollection(((Iterable)list).iterator(),(List)new ArrayList<>(100));
        else if (list instanceof Iterator) return Iterators.addToCollection((Iterator)list,(List)new ArrayList<>(100));
        else if (list.getClass().isArray()) {
            final Object[] objectArray;
            if (list.getClass().getComponentType().isPrimitive()) {
                int length = Array.getLength(list);
                objectArray = new Object[length];
                for (int i = 0; i < length; i++) {
                    objectArray[i] = Array.get(list, i);
                }
            } else {
                objectArray = (Object[]) list;
            }
            List result = new ArrayList<>(objectArray.length);
            Collections.addAll(result, objectArray);
            return result;
        }
        return Collections.singletonList(list);
    }

    public static <T> List<T> take(Iterator<T> iterator, int batchsize) {
        List<T> result = new ArrayList<>(batchsize);
        while (iterator.hasNext() && batchsize-- > 0) {
            result.add(iterator.next());
        }
        return result;
    }

    public static <T> Map<String, T> map(T ... values) {
        Map<String, T> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i+=2) {
            if (values[i] == null) continue;
            map.put(values[i].toString(),values[i+1]);
        }
        return map;
    }

    public static <T> T inTx(GraphDatabaseService db, ThreadPool threadPool, Function<Transaction, T> function) {
        try {

            return threadPool.getExecutorService().submit(() -> {
                try (Transaction tx = db.beginTx()) {
                    T result = function.apply(tx);
                    tx.commit();
                    return result;
                } catch (Exception e) {
                    throw e;
                }
            }).get();

        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate transaction: "+e.getMessage(), e);
        }
    }
}
