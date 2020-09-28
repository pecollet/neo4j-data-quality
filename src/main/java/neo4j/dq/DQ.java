package neo4j.dq;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.time.ZonedDateTime;
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
    @Description("creates a new Data Quality flag on the given node")
    public Stream<FlagResult> createFlag(@Name("node") Node node,
                                         @Name(value="flagLabel", defaultValue = "Generic_Flag")  String flagLabel,
                                         @Name(value="description", defaultValue = "")  String description
            ) {
        //todo : description as complex map/list
        Node root;
        try {
            root = tx.findNode(Label.label("DQ_All"), "class", "all");
            if (root == null) {
                root= tx.createNode(Label.label("DQ_All"), Label.label("DQ_Class"));
                root.setProperty("class", "all");
            }
        } catch (MultipleFoundException mfe) {
            log.error("Multiple roots found : several nodes with label 'DQ_All'.");
            return Stream.of(null);
        }
        Node parent;
        try {
            parent = tx.findNode(Label.label("DQ_Class"), "class", flagLabel);
            if (parent == null) {
                parent= tx.createNode(Label.label("DQ_Class"));
                parent.setProperty("class", flagLabel);
                parent.setProperty("flagCount", 0);
                parent.createRelationshipTo(root, RelationshipType.withName("HAS_DQ_CLASS"));
            }
        } catch (MultipleFoundException mfe) {
            log.error("Multiple parents found : multiple 'DQ_Class' nodes with class='"+flagLabel+"'..");
            return Stream.of(null);
        }
        Node flag= tx.createNode(Label.label(flagLabel), Label.label("DQ_Flag"));
        flag.createRelationshipTo(parent, RelationshipType.withName("HAS_DQ_CLASS"));
        flag.setProperty("description", description);
        flag.setProperty("created", ZonedDateTime.now());
        node.createRelationshipTo(flag, RelationshipType.withName("HAS_DQ_FLAG"));
        //todo : update counts/stats on parent. lock?
        parent.setProperty("flagCount", (Integer)parent.getProperty("flagCount") +1);
        return Stream.of( new FlagResult(flag));
    }

    @Procedure(value = "neo4j.dq.attachToFlag", mode=Mode.WRITE)
    @Description("adds an attachment node to a Data Quality flag")
    public Stream<FlagAttachmentResult> attachToFlag(@Name("flag") Node flag,
                                         @Name("attachmentNode")  Node attachmentNode,
                                         @Name(value="description", defaultValue = "")  String description
    ) {
        Relationship r=flag.createRelationshipTo(attachmentNode, RelationshipType.withName("HAS_ATTACHMENT"));
        r.setProperty("description", description);
        return Stream.of( new FlagAttachmentResult(r));
    }

    @Procedure(value="neo4j.dq.listClasses")
    @Description("list all classes of DQ flags")
    public Stream<ClassResult> listClasses()  {
        ResourceIterator<Node> classNodes=tx.findNodes(Label.label("DQ_Class"));
        return classNodes.stream().map(e -> new ClassResult(e));
    }


    //result type
    public static class FlagResult {
        // yield
        public final Node flag;
        public FlagResult(Node node) {
            this.flag = node;
        }
    }
    public static class ClassResult {
        // yield
        public final Node dqClass;
        public ClassResult(Node node) {
            this.dqClass = node;
        }
    }
    public static class FlagAttachmentResult {
        // yield
        public final Relationship attachment;
        public FlagAttachmentResult(Relationship rel) {
            this.attachment = rel;
        }
    }
}