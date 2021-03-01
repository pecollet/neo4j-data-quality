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
    private static final String TEST_DATA =
        " CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})" +
        " CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})" +
        " CREATE (Carrie:Person {name:'Carrie-Anne Moss', born:1967})" +
        " CREATE (Laurence:Person {name:'Laurence Fishburne', born:1961})" +
        " CREATE (Hugo:Person {name:'Hugo Weaving', born:1960})" +
        " CREATE (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrix)," +
        "        (Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrix)," +
        "        (Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrix)," +
        "        (Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrix)" ;
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, singletonList("neo4j.dq.*"));

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, DQ.class);
        db.executeTransactionally(TEST_DATA);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }


    @Test
    public void testCreateFlag() throws Exception {
        String CREATE_FLAG ="MATCH (a:Person) WHERE a.name='Keanu Reeves' " +
                "CALL neo4j.dq.createFlag(a, 'BadName', 'nodes should not be called A') yield flag " +
                "RETURN  flag";
        db.executeTransactionally(CREATE_FLAG);


        TestUtil.testResult(db, "MATCH (a)-[r:HAS_DQ_FLAG]->(flag:BadName) RETURN flag" , null,
                r -> assertFlagResult(r, "nodes should not be called A") );

        TestUtil.testResult(db, "MATCH (flag:BadName)-[r:HAS_DQ_CLASS]->(class) RETURN class" , null,
                r -> assertClassResult(r, "BadName") );
    }

    @Test
    public void testAttachToFlag() throws Exception {
        String ATTACH_TO_FLAG = "MATCH (a:Person) WHERE a.name='Keanu Reeves' " +
                "MATCH (b:Person) WHERE b.name='Laurence Fishburne' " +
                "CALL neo4j.dq.createFlag(a, 'BadName', 'nodes should not be called A') yield flag " +
                "CALL neo4j.dq.attachToFlag(flag, b, 'attachmentDescription') yield attachment " +
                "RETURN  attachment";
        db.executeTransactionally(ATTACH_TO_FLAG);


        TestUtil.testResult(db, "MATCH (flag)-[attachment:HAS_ATTACHMENT]->(n) RETURN attachment" , null,
                r -> assertAttachmentResult(r, "attachmentDescription") );
    }

    @Test
    public void testDeleteFlags() throws Exception {
        String CREATE_FLAGS = "MATCH (p:Person) " +
                "CALL neo4j.dq.createFlag(p, 'BadName', 'nodes should not be called A') yield flag RETURN flag";
        db.executeTransactionally(CREATE_FLAGS);
        String DELETE_FLAGS = "MATCH (f:DQ_Flag) " +
                "WITH collect(f) as flags " +
                "CALL neo4j.dq.deleteFlags(flags, 2) yield value " +
                "RETURN  value";
        db.executeTransactionally(DELETE_FLAGS);


        TestUtil.testResult(db, "MATCH (flag:DQ_Flag) RETURN flag" , null,
                r -> assertFalse("Expected no results", r.hasNext()) );
    }

    @Test
    public void testDeleteNodeFlags() throws Exception {
        String CREATE_FLAGS = "MATCH (p:Person) " +
                "CALL neo4j.dq.createFlag(p, 'BadName', 'nodes should not be called A') yield flag RETURN flag";
        db.executeTransactionally(CREATE_FLAGS);
        String DELETE_NODE_FLAGS = "MATCH (n)-[:HAS_DQ_FLAG]->(:DQ_Flag) " +
                "WITH collect(n) as nodes " +
                "CALL neo4j.dq.deleteNodeFlags(nodes, 2) yield value " +
                "RETURN  value";
        db.executeTransactionally(DELETE_NODE_FLAGS);


        TestUtil.testResult(db, "MATCH (flag:DQ_Flag) RETURN flag" , null,
                r -> assertFalse("Expected no results", r.hasNext()) );
    }

    @Test
    public void testListFlags() throws Exception {
        String CREATE_FLAGS = "MATCH (p:Person) " +
                "CALL neo4j.dq.createFlag(p, 'BadName', 'desc') yield flag RETURN flag";
        db.executeTransactionally(CREATE_FLAGS);
        String CREATE_FLAGS2 = "MATCH (m:Movie) " +
                "CALL neo4j.dq.createFlag(m, 'BadMovie', 'desc') yield flag RETURN flag";
        db.executeTransactionally(CREATE_FLAGS2);

        TestUtil.testResult(db, "CALL neo4j.dq.listFlags" , null,
                r ->  assertEquals(5, r.stream().count()));
        TestUtil.testResult(db, "CALL neo4j.dq.listFlags('BadMovie') yield flag RETURN flag" , null,
                r ->  assertEquals(1, r.stream().count()));
    }

    @Test
    public void testListClasses() throws Exception {
        String CREATE_FLAGS = "MATCH (p:Person) " +
                "CALL neo4j.dq.createFlag(p, 'BadName', 'desc') yield flag RETURN flag";
        db.executeTransactionally(CREATE_FLAGS);
        String CREATE_FLAGS2 = "MATCH (m:Movie) " +
                "CALL neo4j.dq.createFlag(m, 'BadMovie', 'desc') yield flag RETURN flag";
        db.executeTransactionally(CREATE_FLAGS2);

        TestUtil.testResult(db, "CALL neo4j.dq.listClasses" , null,
                r ->  assertEquals(3, r.stream().count())); // expecting "all" too
        TestUtil.testResult(db, "CALL neo4j.dq.listClasses('BadMovie') yield dqClass RETURN dqClass" , null,
                r ->  assertEquals(1, r.stream().count()));
    }

    @Test
    public void testCreateClass_generic() throws Exception {
        String CREATE_GENERIC_CLASS =  "CALL neo4j.dq.createClass('SomeClass') yield dqClass RETURN dqClass";
        db.executeTransactionally(CREATE_GENERIC_CLASS);

        TestUtil.testResult(db, "MATCH (c:DQ_Class) WHERE c.class='SomeClass' RETURN COUNT(c)" , null,
                r ->  assertEquals(1, r.stream().count()));
        TestUtil.testResult(db, "MATCH (c:DQ_Class) WHERE c.class='SomeClass' RETURN c as class" , null,
                r ->  assertClassResult(r, "SomeClass"));
        //test hierarchy
        TestUtil.testResult(db, "MATCH (c:DQ_Class)-[:HAS_DQ_CLASS]->(p:DQ_Class) WHERE c.class='SomeClass' RETURN c as class, p as parentClass" , null,
                r ->  assertClassHierarchyResult(r, "SomeClass", "all"));
    }

    @Test
    public void testCreateClass_specific() throws Exception {
        String CREATE_CLASS =  "CALL neo4j.dq.createClass('SomeClass', 'ParentClass', 100, 'description') yield dqClass RETURN dqClass";
        db.executeTransactionally(CREATE_CLASS);

        //test created class
        TestUtil.testResult(db, "MATCH (c:DQ_Class) WHERE c.class='SomeClass' RETURN COUNT(c)" , null,
                r ->  assertEquals(1, r.stream().count()));
        TestUtil.testResult(db, "MATCH (c:DQ_Class)-[:HAS_DQ_CLASS]->(p:DQ_Class) WHERE c.class='SomeClass' RETURN c as class" , null,
                r ->  assertClassResult(r, "SomeClass", Long.valueOf(100), "description"));

        //test hierarchy
        TestUtil.testResult(db, "MATCH (c:DQ_Class)-[:HAS_DQ_CLASS]->(p:DQ_Class) WHERE c.class='SomeClass' RETURN c as class, p as parentClass" , null,
                r ->  assertClassHierarchyResult(r, "SomeClass", "ParentClass"));
        TestUtil.testResult(db, "MATCH (c:DQ_Class)-[:HAS_DQ_CLASS]->(p:DQ_Class) WHERE c.class='ParentClass' RETURN c as class, p as parentClass" , null,
                r ->  assertClassHierarchyResult(r, "ParentClass", "all"));
    }

    @Test
    public void testDeleteClass() throws Exception {
        String CREATE_FLAGS = "MATCH (p:Person) " +
                "CALL neo4j.dq.createFlag(p, 'BadName', 'desc') yield flag RETURN flag";
        db.executeTransactionally(CREATE_FLAGS);

        String DELETE_CLASS =  "CALL neo4j.dq.deleteClass('BadName') yield value RETURN value";
        db.executeTransactionally(DELETE_CLASS);


        TestUtil.testResult(db, "MATCH (flag:BadName) RETURN flag" , null,
                r -> assertFalse("Expected no flag results", r.hasNext()) );

        TestUtil.testResult(db, "MATCH (c:DQ_Class{class:'BadName'}) RETURN c" , null,
                r -> assertFalse("Expected no class results", r.hasNext()) );
    }

    @Test
    public void testDeleteClass_hierarchy() throws Exception {
        String CREATE_CLASSES =  "CALL neo4j.dq.createClass('SomeClass', 'ParentClass', 100, 'description') yield dqClass RETURN dqClass";
        db.executeTransactionally(CREATE_CLASSES);

        String CREATE_CHILD_FLAGS = "MATCH (p:Person) " +
                "CALL neo4j.dq.createFlag(p, 'ChildClass', 'desc') yield flag RETURN flag";
        db.executeTransactionally(CREATE_CHILD_FLAGS);
        String CREATE_PARENT_FLAGS = "MATCH (m:Movie) " +
                "CALL neo4j.dq.createFlag(m, 'ParentClass', 'desc') yield flag RETURN flag";
        db.executeTransactionally(CREATE_PARENT_FLAGS);

        String DELETE_CLASS =  "CALL neo4j.dq.deleteClass('ParentClass') yield value RETURN value";
        db.executeTransactionally(DELETE_CLASS);

        //check the correct flags are deleted (and only them)
        TestUtil.testResult(db, "MATCH (flag:ParentClass) RETURN flag" , null,
                r -> assertFalse("Expected no parent flag results", r.hasNext()) );
        TestUtil.testResult(db, "MATCH (flag:ChildClass) RETURN flag" , null,
                r -> assertTrue("Expected children flag results", r.hasNext()) );

        //check the class hierarchy is correct
        TestUtil.testResult(db, "MATCH (c:DQ_Class{class:'ParentClass'}) RETURN c" , null,
                r -> assertFalse("Expected no parent class results", r.hasNext()) );
        TestUtil.testResult(db, "MATCH (c:DQ_Class{class:'ChildClass'}) RETURN c" , null,
                r -> assertTrue("Expected child class results", r.hasNext()) );
        TestUtil.testResult(db, "MATCH (c:DQ_Class{class:'ChildClass'})-[:HAS_DQ_CLASS]->(root:DQ_Class{class:'all'}) RETURN c" , null,
                r -> assertTrue("Expected child class re-attached to root", r.hasNext()) );
    }

    @Test
    public void testStatistics() throws Exception {
        String CREATE_CLASS =  "CALL neo4j.dq.createClass('SomeClass', 'ParentClass', 100, 'description') yield dqClass RETURN dqClass";
        db.executeTransactionally(CREATE_CLASS);
        String CREATE_FLAGS = "MATCH (p:Person) " +
                "CALL neo4j.dq.createFlag(p, 'SomeClass', 'desc') yield flag RETURN flag";
        db.executeTransactionally(CREATE_FLAGS);


        TestUtil.testResult(db, "call neo4j.dq.statistics" , null,
                r -> {Long[] expected={0L,4L,4L}; assertStatsResult(r, "all", expected);}
        );

        TestUtil.testResult(db, "call neo4j.dq.statistics('ParentClass') YIELD dqClass, directFlagCount, indirectFlagCount, totalFlagCount RETURN *" , null,
                r -> {Long[] expected={0L,4L,4L}; assertStatsResult(r, "ParentClass", expected);}
        );
        TestUtil.testResult(db, "call neo4j.dq.statistics('SomeClass') YIELD dqClass, directFlagCount, indirectFlagCount, totalFlagCount RETURN *" , null,
                r -> {Long[] expected={4L,0L,4L}; assertStatsResult(r, "SomeClass", expected);}
        );
    }

    @Test
    public void testStatistics_empty() throws Exception {
        TestUtil.testResult(db, "call neo4j.dq.statistics" , null,
                r -> assertFalse("Expected no parent flag results", r.hasNext()) );
    }

    private void assertFlagResult(Result r, String expectedDescription) {
        Node flag;
        for (Map<String, Object> map : Iterators.asList(r)) {
            flag = (Node) map.get("flag");

            assertTrue("expected DQ_FLAG label", flag.hasLabel(DQ.DQ_FLAG));

            String description = flag.getProperty(DQ.descriptionProperty).toString();
            assertEquals(expectedDescription, description);

            assertTrue("expected created property", flag.hasProperty(DQ.createdProperty));

            assertTrue("expected HAS_DQ_CLASS relationship", flag.hasRelationship(Direction.OUTGOING, DQ.HAS_DQ_CLASS));
        }
    }
    private void assertClassResult(Result r, String expectedClassProperty) {
        assertClassResult(r, expectedClassProperty, null, null);
    }
    private void assertClassResult(Result r, String expectedClassProperty, Long expectedLimit, String expectedDescription) {
        Node classNode;
        for (Map<String, Object> map : Iterators.asList(r)) {
            classNode = (Node) map.get("class");

            assertTrue("expected DQ_CLASS label",classNode.hasLabel(DQ.DQ_CLASS));

            String classProperty = classNode.getProperty(DQ.classProperty).toString();
            assertEquals("expected matching class property", expectedClassProperty, classProperty);

            assertTrue(classNode.hasRelationship(Direction.OUTGOING, DQ.HAS_DQ_CLASS));

            if (!Util.isNullOrEmpty(expectedDescription)) {
                String descProperty = classNode.getProperty(DQ.descriptionProperty).toString();
                assertEquals("expected matching description property", expectedDescription, descProperty);
            }
            if (expectedLimit != null) {
                Long limitProperty = (Long)classNode.getProperty(DQ.alertTriggerLimitProperty);
                assertEquals("expected matching alertTriggerLimit property", expectedLimit, limitProperty);
            }
        }
    }
    private void assertAttachmentResult(Result r, String expectedDescription) {
        Relationship rel;
        for (Map<String, Object> map : Iterators.asList(r)) {
            rel = (Relationship) map.get("attachment");

            assertTrue("expected HAS_ATTACHMENT relationship", rel.isType(DQ.HAS_ATTACHMENT));

            String description = rel.getProperty(DQ.descriptionProperty).toString();
            assertEquals(expectedDescription, description);

        }
    }
    private void assertClassHierarchyResult(Result r, String expectedClassProperty, String expectedParentClassProperty) {
        Node classNode, parentClassNode;
        for (Map<String, Object> map : Iterators.asList(r)) {
            classNode = (Node) map.get("class");
            parentClassNode = (Node) map.get("parentClass");

            assertTrue("expected DQ_CLASS label",classNode.hasLabel(DQ.DQ_CLASS));
            assertTrue("expected DQ_CLASS label",parentClassNode.hasLabel(DQ.DQ_CLASS));

            String classProperty = classNode.getProperty(DQ.classProperty).toString();
            assertEquals(expectedClassProperty, classProperty);
            String parentClassProperty = parentClassNode.getProperty(DQ.classProperty).toString();
            assertEquals(expectedParentClassProperty, parentClassProperty);

            assertTrue("expected HAS_DQ_CLASS relationship", classNode.hasRelationship(Direction.OUTGOING, DQ.HAS_DQ_CLASS));
        }
    }
    private void assertStatsResult(Result r, String expectedClass, Long[] expectedStats) {
        if (!r.hasNext()) assert(false);
        Map res = r.next();
        assertEquals("expected dqClass column", expectedClass, res.get("dqClass")) ;
        assertEquals("expected directFlagCount column", expectedStats[0], res.get("directFlagCount")) ;
        assertEquals("expected indirectFlagCount column", expectedStats[1], res.get("indirectFlagCount")) ;
        assertEquals("expected totalFlagCount column", expectedStats[2], res.get("totalFlagCount")) ;
    }

}
