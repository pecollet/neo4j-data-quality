package neo4j.dq;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;
import org.neo4j.graphdb.Node;
import static org.neo4j.graphdb.Direction.*;


public class DQ  {



    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Procedure(value = "neo4j.dq.createFlag", mode=Mode.WRITE)
    @Description("")
    public Stream<FlagResult> createFlag(@Name("node") Node node,
                                         @Name(value="flagLabel", defaultValue = "DQ_Flag")  String flagLabel,
                                         @Name(value="description", defaultValue = "")  String description
            ) {
        //todo : multiple labels, multiple nodes sharing a flag
        Node root;
        try {
            root = tx.findNode(Label.label("DQ"), "name", "root");
            if (root == null) {
                root= tx.createNode(Label.label("DQ"));
                root.setProperty("name", "root");
            }
        } catch (MultipleFoundException mfe) {
            log.error("Multiple roots found : clean up nodes with label 'DQ' named 'root'.");
            return Stream.of(null);
        }
        Node flag= tx.createNode(Label.label(flagLabel));
        flag.createRelationshipTo(root ,RelationshipType.withName("SUBCLASS_OF"));
        flag.setProperty("description", description);
        node.createRelationshipTo(flag, RelationshipType.withName("HAS_DG_FLAG"));
        return Stream.of( new FlagResult(flag));
    }




    //result type
    public static class FlagResult {
        // yield
        public final Node flag;


        public FlagResult(Node node) {
            this.flag = node;
        }
    }
}