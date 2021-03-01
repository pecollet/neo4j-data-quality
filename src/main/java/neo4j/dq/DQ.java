package neo4j.dq;

import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.time.ZonedDateTime;

import java.util.*;
import java.util.stream.Stream;
import org.neo4j.graphdb.Node;



public class DQ  {
    public static final Label DQ_FLAG = Label.label("DQ_Flag");
    public static final Label DQ_CLASS = Label.label("DQ_Class");

    public static final RelationshipType HAS_DQ_CLASS = RelationshipType.withName("HAS_DQ_CLASS");
    public static final RelationshipType HAS_DQ_FLAG = RelationshipType.withName("HAS_DQ_FLAG");
    public static final RelationshipType HAS_ATTACHMENT = RelationshipType.withName("HAS_ATTACHMENT");

    public static final String classProperty = "class" ;
    public static final String createdProperty = "created" ;
    public static final String descriptionProperty = "description" ;
    public static final String alertTriggerLimitProperty = "alertTriggerLimit" ;

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Context
    public ThreadPool threadPool;



    @Procedure(value = "neo4j.dq.createFlag", mode=Mode.WRITE)
    @Description("creates a new Data Quality flag on the given node")
    public Stream<FlagResult> createFlag(@Name("node") Node node,
                                         @Name(value="flagLabel", defaultValue = "Generic_Flag")  String flagLabel,
                                         @Name(value="description", defaultValue = "")  String description
            ) {

        Node parent = findOrCreateClassNode(flagLabel);
        if (parent == null ) return Stream.of(null);

        Node flag= tx.createNode(Label.label(flagLabel), DQ_FLAG);
        flag.createRelationshipTo(parent, HAS_DQ_CLASS);
        flag.setProperty(descriptionProperty, description);
        flag.setProperty(createdProperty, ZonedDateTime.now());
        node.createRelationshipTo(flag, HAS_DQ_FLAG);
        //to do : update counts/stats on parent. lock?
        //parent.setProperty("flagCount", (Integer)parent.getProperty("flagCount") +1);
        return Stream.of( new FlagResult(flag));
    }

    @Procedure(value = "neo4j.dq.attachToFlag", mode=Mode.WRITE)
    @Description("adds an attachment node to a Data Quality flag")
    public Stream<FlagAttachmentResult> attachToFlag(@Name("flag") Node flag,
                                         @Name("attachmentNode")  Node attachmentNode,
                                         @Name(value="description", defaultValue = "")  String description
    ) {
        Relationship r=flag.createRelationshipTo(attachmentNode, HAS_ATTACHMENT);
        r.setProperty(descriptionProperty, description);
        return Stream.of( new FlagAttachmentResult(r));
    }

    @Procedure(value = "neo4j.dq.deleteFlags", mode=Mode.WRITE)
    @Description("deletes Data Quality flags")
    public Stream<LongResult> deleteFlags(@Name("flags") Object flags,
                                          @Name(value="batchSize", defaultValue="1") long batchSize) {
        Iterator<Node> it = Util.convertToList(flags).stream().map(id -> Util.node(tx, id)).filter(node -> ((Node) node).hasLabel(DQ_FLAG)).iterator();
        long count = 0;
        while (it.hasNext()) {
            final List<Node> batch = Util.take(it, (int)batchSize);
            count += Util.inTx(db, threadPool,
                        (txInThread) -> {
                            txInThread.execute("FOREACH (n in $nodes | DETACH DELETE n)", Util.map("nodes", batch))
                                    .close();
                            return batch.size();
                        });
        }
        return Stream.of(new LongResult(count));
    }

    @Procedure(value = "neo4j.dq.deleteNodeFlags", mode=Mode.WRITE)
    @Description("deletes all Data Quality flags linked to the given nodes")
    public Stream<LongResult> deleteNodeFlags(@Name("nodes") Object nodes,
                                          @Name(value="batchSize", defaultValue="1") long batchSize) {

        Iterator<Node> it = Util.convertToList(nodes).stream().map(id -> Util.node(tx, id)).iterator();

        String query = "UNWIND $nodes as n "
                    + "MATCH (n)-[:"+HAS_DQ_FLAG.toString()+"]->(flag:"+DQ_FLAG.toString()+") "
                    + "DETACH DELETE flag ";

        long count = 0;
        while (it.hasNext()) {
            final List<Node> batch = Util.take(it, (int)batchSize);

            count += Util.inTx(db, threadPool,
                    (txInThread) -> {
                        txInThread.execute(query, Util.map("nodes", batch))
                                .close();
                        return batch.size();
                    });
        }
        return Stream.of(new LongResult(count));
    }

    @Procedure(value="neo4j.dq.listFlags")
    @Description("list DQ flags")
    public Stream<FlagResult> listFlags(@Name(value="filter", defaultValue="") String filter)  {
        if (Util.isNullOrEmpty(filter)) {
            return tx.findNodes(DQ_FLAG).stream().map(c -> new FlagResult(c));
        } else {
            return tx.findNodes(DQ_FLAG).stream().filter(c -> c.hasLabel(Label.label(filter))).map(c -> new FlagResult(c));
        }
    }

    @Procedure(value="neo4j.dq.listClasses")
    @Description("list all classes of DQ flags")
    public Stream<ClassResult> listClasses(@Name(value="filter", defaultValue="") String filter)  {
        if (Util.isNullOrEmpty(filter)) {
            return tx.findNodes(DQ_CLASS).stream().map(c -> new ClassResult(c));
        } else {
            return tx.findNodes(DQ_CLASS).stream().filter(c -> c.getProperty(classProperty).equals(filter)).map(c -> new ClassResult(c));
        }
    }

    @Procedure(value="neo4j.dq.createClass", mode = Mode.WRITE)
    @Description("Create a DQ class")
    public Stream<ClassResult> createClass(@Name("class") String dqClass,
                                           @Name(value="parentClass", defaultValue = "all") String parentClass,
                                           @Name(value="alertTriggerLimit", defaultValue = "-1") Long alertTriggerLimit,
                                           @Name(value="description", defaultValue = "") String description)  {
        Node classNode = findOrCreateClassNode(dqClass, parentClass);
        if (alertTriggerLimit > 0) classNode.setProperty(alertTriggerLimitProperty, alertTriggerLimit);
        if (!Util.isNullOrEmpty(description)) classNode.setProperty(descriptionProperty, description);
        return Stream.of(new ClassResult(classNode));
    }

    @Procedure(value="neo4j.dq.deleteClass", mode = Mode.WRITE)
    @Description("Deletes a DQ class and all its flags.")
    public Stream<LongResult> deleteClass(@Name("class") String dqClass) throws Exception {
        Node classNode;
        try {
            classNode = tx.findNode(DQ_CLASS, classProperty, dqClass);
        } catch (MultipleFoundException mfe) {
            log.error("Found multiple 'DQ_Class' nodes with class='"+dqClass+"'.");
            throw mfe;
        }
        if (classNode == null) return Stream.of(null);
        Iterator<Relationship> flagRels = classNode.getRelationships(Direction.INCOMING, HAS_DQ_CLASS).iterator();

        //delete all flags
        long count = 0;
        while (flagRels.hasNext()) {
            Relationship flagRel = flagRels.next();
            Node flag = flagRel.getStartNode();
            if (flag.hasLabel(DQ_FLAG))
                flag.getRelationships().forEach(Relationship::delete);
                flag.delete();
                count+=1;
        }
        //delete the class
        classNode.getRelationships().forEach(Relationship::delete);
        classNode.delete();
        return Stream.of(new LongResult(count));
    }

    @Procedure(value="neo4j.dq.statistics")
    @Description("Computes statistics about DQ flags in the graph")
    public Stream<StatsResult> statistics(@Name(value="filter", defaultValue="") String filter) throws Exception {
        //TODO : NPE here when no DQ_CLass
        if (Util.isNullOrEmpty(filter)) filter= "all";
        Node root;
        try {
            root = tx.findNode(DQ_CLASS, classProperty, filter);
        } catch (MultipleFoundException mfe) {
            log.error("Found multiple 'DQ_Class' nodes with class='"+filter+"'.");
            throw mfe;
        }
        if (root == null) return Stream.of(null);

        String dqClass = (String)root.getProperty(classProperty);
        Map<String, Long> result = countChildrenFlags(root);
        return Stream.of(new StatsResult(dqClass, result.get("direct"), result.get("indirect")));
    }

    private Map countChildrenFlags(Node classNode) {
        long directChildren = 0 ;
        long indirectChildren = 0;

        Iterator<Relationship> classRels= classNode.getRelationships(Direction.INCOMING, HAS_DQ_CLASS).iterator();

        while (classRels.hasNext()) {
            Relationship classRel = classRels.next();
            Node startNode = classRel.getStartNode();
            if (startNode.hasLabel(DQ_FLAG)) {
                directChildren+=1;
            } else {
                Map<String, Long> x = countChildrenFlags(startNode);
                indirectChildren += x.get("total");
            }
        }
        HashMap<String, Long> stats = new HashMap<>();
        stats.put("direct", directChildren);
        stats.put("indirect", indirectChildren);
        stats.put("total", directChildren + indirectChildren);

        return stats ;
    }
    private Node findOrCreateClassNode(String label) {
        return findOrCreateClassNode(label, null);
    }
    private Node findOrCreateClassNode(String label, String parentLabel) {
        if (Util.isNullOrEmpty(parentLabel)) parentLabel="all";
        Node classNode;

        try {
            classNode = tx.findNode(DQ_CLASS, classProperty, label);
            if (classNode == null) {
                classNode= tx.createNode(DQ_CLASS);
                classNode.setProperty(classProperty, label);
                //classNode.setProperty("flagCount", 0);
                if (label != "all") {
                    Node parent = findOrCreateClassNode(parentLabel, "all");
                    if (parent != null )  classNode.createRelationshipTo(parent, HAS_DQ_CLASS);
                }
            }
            return classNode;
        } catch (MultipleFoundException mfe) {
            log.error("Multiple parents found : multiple 'DQ_Class' nodes with class='"+label+"'.");
            return null;
        }
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

    public static class LongResult {
        public final Long value;

        public LongResult(Long value) {
            this.value = value;
        }
    }
    public static class StatsResult {
        public final String dqClass;
        public final Long directFlagCount;
        public final Long indirectFlagCount;
        public final Long totalFlagCount;

        public StatsResult(String dqClass, Long directFlagCount, Long indirectFlagCount) {
            this.dqClass= dqClass ;
            this.directFlagCount = directFlagCount;
            this.indirectFlagCount = indirectFlagCount;
            this.totalFlagCount = directFlagCount + indirectFlagCount;
        }
    }
}