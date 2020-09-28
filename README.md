# neo4j.data.quality
stored procedures to perform crate & manage data quality flags

## Procedure **neo4j.dq.createFlag**
Creates a flag on a given node.

### Usage
` CALL neo4j.dq.createFlag(node, label, description)`
### parameters 
* _node_ : `Node`
* _label_ : string for the DQ flag label, used to categorise the type of data quality issue
* _description_ : string
* Returns the impacted nodes and their state.

Returns the flag node.

### examples
**Impacts from a single node, using the default :IMPACTS relationship type, and default limits :**
```
MATCH (a:Node) WHERE a.name='A'  
CALL neo4j.dq.createFlag(a, '') yield flag
RETURN  node, flag
```


## Procedure **neo4j.dq.attachToFlag**
Attach node(s) to a flag.

### Usage
` CALL neo4j.dq.attachToFlag(flagNode, node| list of nodes)`
### parameters 