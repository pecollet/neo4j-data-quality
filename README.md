# neo4j.data.quality
stored procedures to create & manage data quality flags.

## Installation
* Copy the jar into Neo4j's "plugins" directory : `$NEO4J_HOME/plugins`
* For security reasons, procedures that use internal APIs are disabled by default. They can be enabled by specifying config in `$NEO4J_HOME/conf/neo4j.conf` e.g. `dbms.security.procedures.unrestricted=neo4j.dq.*` 
* Restart Neo4j

## Definitions
* _flag_ : modelled as a `DQ_Flag` node linked to a data node, representing a data quality issue affecting that node.
* _class_ : to help organize flags, they're given a class, which is part of a class hierarchy. The flag class is modelled as an extra node label on the flag, as well as a separate `DQ_Class` node linked to the flag with a `HAS_DQ_FLAG` relationship. Classes in the hierarchy are linked to their children classes with a `HAS_DQ_CLASS` relationship. 
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
Creates a `DQ_Flag` node and links it to the given node with a `HAS_DQ_FLAG` relationship.
Also links the flag node with its `DQ_Class` node via a `HAS_DQ_CLASS` relationship. If the class node doesn't exist, it is created as a child of the root class node.

### Usage
` CALL neo4j.dq.createFlag(node, label, description)`
### parameters 
* _node_ : `Node` the data node to flag. 
* _label_ : string for the DQ flag label, used to categorise the type of data quality issue. Also used for the "DQ_Class" node. 
* _description_ : string property of the flag node.
### output
Returns the created flag node
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
* _flag_ : `Node` the flag node to attach to
* _attachmentNode_ : `Node` the data node to attach
* _description_ : string property of the `HAS_ATTACHMENT` relationship.
### output
Returns the `HAS_ATTACHMENT` relationship created.
### examples


## Procedure **neo4j.dq.deleteFlags**
Deletes flag nodes. 

### Usage
` CALL neo4j.dq.deleteFlags(flags)`
### parameters
* _flags_ : ANY (node|[nodes]|id|[ids]). List of flag nodes (or their ids) to delete. Any node passed in that's not a `DQ_Flag`, will be ignored.
* _batchSize_ : long. Size of transaction batches for deletions.
### output
Returns the number of deleted flags.
### examples


## Procedure **neo4j.dq.deleteNodeFlags**
Deletes all flag node linked to a data node.

### Usage
` CALL neo4j.dq.deleteNodeFlags(nodes)`
### parameters
* _nodes_ : ANY (node|[nodes]|id|[ids]). List of nodes (or their ids) whose linked flags must be deleted.
### output
Returns the number of deleted flags.
### examples


## Procedure **neo4j.dq.listFlags**
Lists flag nodes

### Usage
` CALL neo4j.dq.listFlags(filter)`
### parameters
* _filter_ : string. Label of the flag node.
### output
Returns the `DQ_FLAG` nodes.
### examples


## Procedure **neo4j.dq.listClasses**
List all existing DQ classes in the graph.

### Usage
` CALL neo4j.dq.listClasses`
### parameters
* _filter_ : string. Name of the class.
### output
Returns all the `DQ_Class` nodes.
### examples


## Procedure **neo4j.dq.createClass**
Creates a new DQ class.

### Usage
` CALL neo4j.dq.createClass(class, parentClass, alertTriggerLimit, description)`
### parameters
* _class_ : string. Label to associate with flags of that class.
* _parentClass_ : string. Parent DQ_Class. Defaults to `all`, the root of the class hierarchy.
* _alertTriggerLimit_ : int. Limit above which the count of children flags should trigger an alert.
* _description_ : string
### output
Returns the `DQ_Class` node.
### examples


## Procedure **neo4j.dq.deleteClass**
Deletes a DQ class and all its flags.

### Usage
` CALL neo4j.dq.deleteClass(class)`
### parameters
* _class_ : string. Label to associate with flags of that class.
### output
Returns the number of deleted flags.
### examples


## Procedure **neo4j.dq.statistics**
Computes statistics about DQ flags in the graph.

### Usage
` CALL neo4j.dq.statistics(filter)`
### parameters
* _filter_ : string.
### output
Returns the counts of number of flags per DQ_Class
### examples

