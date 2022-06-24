# CASH - Cascading Shell

CASH is a utility for administrators of large computer clusters to quickly run shell commands 
on all or a subset of the cluster nodes. CASH generates a cascading, or tree-like topology of the nodes,
and is therefore much faster than other tools that simply iterate the nodes or try to access many nodes in parallel.

CASH is supposed to be run from the administrator's machine, but may also be run from one of the cluster nodes. In
the first case, all communication between the computer cluster and the admin machine is channelled over a gateway host.

Please see below for the execution/communication model.

## Requirements

CASH has the following requirements:

- python > 3.6 on each node
- password-less SSH access to and between all nodes

## Setup

Please run `pip install cascading-shell` and use the `cash` command line tool on the admin machine. Then, configure your
cluster(s). Nodes and nodegroups are configured in `~/.cash.topo.json` like this:

```json
{
  "nodes": {
    "group1": "clus1node001,clus1node002,clus1node003",
    "all": {
      "site1": {
        "cluster1": {
          "rack1": "clus1node[001-020]",
          "rack2": "clus1node[021-040]",
          "rack3": "clus1node[041-060]"
        },
        "cluster2": {
          "rack1": "clus2node[001-020]",
          "rack2": "clus2node[021-040]",
          "rack3": "clus2node[041-060]"
        }
      },
      "site2": {
        "cluster3": {
          "rack1": "clus3node[001-020]",
          "rack2": "clus3node[021-040]",
          "rack3": "clus3node[041-060]"
        },
        "cluster4": {
          "rack1": "clus4node[001-020]",
          "rack2": "clus4node[021-040]",
          "rack3": "clus4node[041-060]"
        },
        "cluster5": "clus5node[001-020]"
      }
    }
  }
}
```

The config file has the following rules:

- Right now, everything lives under the `nodes` object. 
- The file format is standard JSON, where each key is a group name and each value is a comma separated list of nodes.
- Nodes with sequential numbers can be shortened using square brackets, e.g., `node[001-003]` resolves
  to `node001,node002,node003`. Be careful with leading zeros here! You may also use a comma here, such as: 
  `node[001-003,005]` -> `node001,node002,node003,node005`. You can also use multiple bracket instances: 
  `clus[1-3]node[001-003]` -> `clus1node001,clus1node002,clus1node003,clus2node001,clus2node002,clus2node003,clus3node001,clus3node002,clus3node003` and so on.
- Groups can be nested. The topology of the node tree is specified in the mandatory `all` group. It is wise to reflect 
  network latency/bandwidth in the tree; for instance, as in the above example, you may divide your HPC into groups of 
  site, cluster, rack if applicable. 
- Aside from `all`, you can specify as many groups as you wish and nest them to your liking. 

## Cascading communication model

CASH communicates with each node in a cascading fashion, where CASH itself on each node acts as a proxy for its 
immediate children and forwards all messages from the children to its parent and vice versa. Let's try to understand 
this with an example. Imaging the following topology configuration:

```json
{
  "nodes": {
    "all": {
      "site1": {
        "cluster1": {
          "rack1": "clus1node[1-3]",
          "rack2": "clus1node[4-6]"
        },
        "cluster2": {
          "rack1": "clus2node[1-3]",
          "rack2": "clus2node[4-6]"
        }
      },
      "site2": {
        "cluster3": {
          "rack1": "clus3node[1-3]",
          "rack2": "clus3node[4-6]"
        },
        "cluster4": {
          "rack1": "clus4node[1-3]",
          "rack2": "clus4node[4-6]"
        }
      }
    }
  }
}
```

We have a total of four clusters in two geographical sites, each cluster has two racks with three nodes each. We now 
want to execute a command on all nodes using CASH. First, CASH spawns an instance of itself on the gateway host, that
can be specified via the `DEFAULT_JUMP_HOST` variable or via the command line parameter `--jump-host`. From the gateway,
a connection to the first host of `site1` and the first host of `site2` is established, i.e., `clus1node1` and 
`clus3node1`. From each of those two nodes, CASH hops to the first node of each cluster (e.g., `clus1node2` for 
`cluster1`, as `clus1node1` was already used, and `clus2node1`), from there to the first 
node of each rack, and then to the remaining nodes. 

For example, `clus4node5` is reached in the following way: 
`ADMIN_MACHINE -> gateway -> clus3node1 (site) -> clus4node1 (cluster) -> clus4node4 (rack) -> clus4node5 (node)`. This 
tiered or cascading execution model of course makes sense only for a larger number of nodes than in this example. You
can tell CASH to use a flat instead of cascading connection model with the `--flatten` parameter. 

The number of parallel connections on each node is limited by the `--fan-size` parameter (env `DEFAULT_FANSIZE = 50`).
When more that FANSIZE nodes are direct children of one node, they are grouped by FANSIZE and an additional layer is 
formed. 

Every node that is part of the tree receives and forwards messages from/to its parent and its children, and also 
executes the desired shell command locally. 

## Usage

Here is a copy of `cash --help`:

```
usage: cash [-h] [-n NODES] [--jumphost JUMPHOST] [--ssh-timeout SSH_TIMEOUT]
            [-s FANSIZE] [--flatten] [-p] [--json | --shell | --quiet]
            {run,plan} ...

positional arguments:
  {run,plan}            Please use one of the following sub commands
    run                 Run command
    plan                Print tree as json to stdout (view with, e.g.,
                        firefox)

optional arguments:
  -h, --help            show this help message and exit
  -n NODES, --nodes NODES
                        Node or node groups.
  --jumphost JUMPHOST   Gateway host to cluster.
  --ssh-timeout SSH_TIMEOUT
                        Define a timeout for SSH sessions. 0 = no timeout
  -s FANSIZE, --fansize FANSIZE
                        Maximum number of parallel SSH sessions.
  --flatten             Disable tree mode.
  -p, --progress        Show progress of received answers
  --json                JSON output format
  --shell               Shell friendly output format
  --quiet               No output
```

- Node groups can be specified with `@group_name` in the `--nodes` parameter. 
- You can exclude hosts by using `-n "@group,-node01"`.
- You can use the square bracket syntax here, too: `-n "node[1-9]"`.

You can specify the defaults of the CLI parameter via the following environment variables:

```python
DEFAULT_SSH_TIMEOUT = 30
DEFAULT_FANSIZE = 50
DEFAULT_NODES_STRING = "@all"
DEFAULT_OUT_FORMAT = "text"
DEFAULT_JUMP_HOST = "jumphost"
DEFAULT_FLATTEN = False
DEFAULT_RUN_SHELL = True
```
