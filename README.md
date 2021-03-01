# neo4j.data.quality
stored procedures to create & manage data quality flags.

## Installation
* Copy the jar into Neo4j's "plugins" directory : `$NEO4J_HOME/plugins`
* For security reasons, procedures that use internal APIs are disabled by default. They can be enabled by specifying config in `$NEO4J_HOME/conf/neo4j.conf` e.g. `dbms.security.procedures.unrestricted=neo4j.dq.*` 
* Restart Neo4j

## Definitions
* _flag_ : modelled as a `DQ_Flag` node, linked to a data node with a `HAS_DQ_FLAG` relationship, representing a data quality issue affecting that node.
* _class_ : to help organize flags, they're given a class, which is part of a class hierarchy. The flag class is modelled as an extra node label on the flag, as well as a separate `DQ_Class` node linked to the flag with a `HAS_DQ_CLASS` relationship. Classes in the hierarchy are linked to their children/parent classes with a `HAS_DQ_CLASS` relationship. 
* _attachment_ : to provide more context to a DQ flag, one can attach other nodes to it (beyond the node it already links to). For example, a flag could represent a data mismatch between 2 nodes, in which case it can be useful to attach the second node to the flag.

## Features

The following procedures are exposed :
* neo4j.dq.createFlag
* neo4j.dq.attachToFlag
* neo4j.dq.deleteFlag
* neo4j.dq.deleteNodeFlags
* neo4j.dq.listFlags
* neo4j.dq.listClasses
* neo4j.dq.createClass
* neo4j.dq.deleteClass
* neo4j.dq.statistics


## Procedure **neo4j.dq.createFlag**
Creates a "data quality flag" node for the given data node.

### Usage
` CALL neo4j.dq.createFlag(node, label, description)`
### parameters 
* _node_ (`Node`|id) : the data node to flag. 
* _label_ (String) : label for the flag node, used to categorise the type of data quality issue. Also used for the "DQ_Class" node. Optional (defaults to "Generic_Flag").
* _description_ (string) : property of the flag node. Optional (defaults to "").
### output
* Creates a `(flag:DQ_Flag:_label_)` node, with the provided `description=_description_` as property, and the relationship `(_node_)-[:HAS_DQ_FLAG]->(flag)`.
* May also create, if it doesn't exist already, a `(class:DQ_Class)` node, with `class=_label_` as property, and the relationship `(flag)-[:HAS_DQ_CLASS]->(class)`.
* If the class node doesn't exist, it is created as a child of the root class node : `(class)-[:HAS_DQ_CLASS]->(root)`.
* Returns the created flag node
### examples
**Flag all `Node` nodes that are missing a "state" property :**
```
MATCH (n:Node) WHERE NOT EXISTS n.state 
CALL neo4j.dq.createFlag(n, 'MissingState', 'nodes of type Node should have a state property') yield flag
RETURN flag
```


## Procedure **neo4j.dq.attachToFlag**
Attach a data node to a flag with a `HAS_ATTACHMENT` relationship.

### Usage
` CALL neo4j.dq.attachToFlag(flagNode, node| list of nodes)`
### parameters 
* _flag_ (`Node`|id) : the flag node to attach to
* _attachmentNode_ (`Node`|id) : the data node to attach
* _description_ (String) : property of the `HAS_ATTACHMENT` relationship. Optional (defaults to "").
### output
* Creates a relationship `(_flag_)-[:HAS_ATTACHMENT]->(_node_)`, with `description=_description_` as property of the relationship.
* Returns the `HAS_ATTACHMENT` relationship created.
### examples


## Procedure **neo4j.dq.deleteFlags**
Deletes flag nodes. 

### Usage
` CALL neo4j.dq.deleteFlags(flags)`
### parameters
* _flags_ (ANY: `Node`|[`Node`]|id|[ids]) : Node or list of nodes (or its/their ids) of the flag(s) to delete. Any node passed in that's not a `DQ_Flag`, will be ignored.
* _batchSize_ (Long) : Size of transaction batches for deletions. Optional (defaults to 1).
### output
* Performs a DETACH DELETE of the provided flag nodes, batched in several transactions.
* Returns the number of deleted flags.
### examples


## Procedure **neo4j.dq.deleteNodeFlags**
Deletes all flag nodes linked to a data node.

### Usage
` CALL neo4j.dq.deleteNodeFlags(nodes)`
### parameters
* _nodes_ (ANY: `Node`|[`Node`]|id|[ids]) : Node or list of nodes (or its/their ids) whose linked flags must be deleted.
* _batchSize_ (Long) : Size of transaction batches for deletions. Optional (defaults to 1).
### output
* Performs a DETACH DELETE of the flag nodes, batched in several transactions.
* Returns the number of deleted flags.
### examples


## Procedure **neo4j.dq.listFlags**
Lists flag nodes.

### Usage
` CALL neo4j.dq.listFlags(filter)`
### parameters
* _filter_ (String) : flag class for filtering results. Optional (defaults to returning all flags).
### output
Returns the flag nodes.
### examples


## Procedure **neo4j.dq.listClasses**
List DQ classes.

### Usage
` CALL neo4j.dq.listClasses(filter)`
### parameters
* _filter_ (String) : flag class for filtering results. Optional (defaults to returning all classes).
### output
Returns `DQ_Class` nodes.
### examples


## Procedure **neo4j.dq.createClass**
Creates a new DQ class.

### Usage
` CALL neo4j.dq.createClass(class, parentClass, alertTriggerLimit, description)`
### parameters
* _class_ (String) : Name of the DQ class.
* _parentClass_ (String) : Parent DQ_Class. Optional (defaults to "all", the root of the class hierarchy).
* _alertTriggerLimit_ (Long) : Limit above which the count of children flags should trigger an alert (Not yet implemented). Optional (defaults to -1).
* _description_ (String) : property of the class node. Optional (defaults to "").
### output
Returns the created class node.
### examples


## Procedure **neo4j.dq.deleteClass**
Deletes a class and all its flags.

### Usage
` CALL neo4j.dq.deleteClass(class)`
### parameters
* _class_ (String) : Name of the DQ class.
### output
* Deletes all the children flags of that class. 
* Any child class is kept, and re-attached to the root class.
* Deletes the class node.  
* Returns the number of deleted flags.
### examples


## Procedure **neo4j.dq.statistics**
Computes statistics about DQ flags in the graph.

### Usage
` CALL neo4j.dq.statistics(filter)`
### parameters
* _filter_ (String) : class name for which to compute statistics.
### output
Returns the counts of number of direct/indirect/total children flags for the class
### examples

