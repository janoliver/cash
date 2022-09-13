#!/bin/env python
# -*- coding: utf-8 -*-

"""
Helper classes and functions to manage a set of node names and 
node groups. Some functions to read the `.topo.yml` config file
and build the group and node topology tree (and rebalance it) are
also included.
"""

import collections.abc
import itertools
import json
import os
import re

from cash.utils import ROOT

from cash.config import DEFAULT_JUMP_HOST


class Node(object):
    def __init__(self, name, children=None, parent=None):
        self._children = set()
        self._parent = None

        self.name = name
        self.parent = parent
        self.children = children or []

    @property
    def children(self):
        return list(self._children)

    @children.setter
    def children(self, child_list):
        for c in child_list:
            self._children.add(c)
            c._parent = self

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, parent):
        #self._parent = parent

        if parent:
            parent._children.add(self)

        # remove this node from the children lists of the former parent
        if self._parent:
            self._parent._children.remove(self)

        self._parent = parent

    def is_root(self):
        return self.parent is None

    def is_leaf(self):
        return bool(self.children)


class NodeGroup(Node):
    """
    This class holds a group of nodes. While `NodeSet()` (see below) is a
    flat container of host names, and nothing more, `NodeGroup()` is 
    hierarchical (it has children and a parent) and can be reference by
    its name (say, `@all`). The attribute `node_group.nodeset` is a lazily
    evaluated `NodeSet` instance with all the group's nodes (including the
    nodes of children groups!).
    """
    registry = {}
    nodegroup_def_re = re.compile(r"(?:(?:\w+\[[\d,-]+\])+|@\w+|\w+)")

    def __init__(self, name, contents=None, **kwargs):
        super(NodeGroup, self).__init__(name, **kwargs)

        self.contents = []
        self.nodeset_cache = None

        if contents:
            self.add(contents)

    def add(self, obj):
        # add something to the NodeGroup. Can be a string, a NodeGroup or a
        # NodeSet, and iterables of each of those.
        self.nodeset_cache = None

        if isinstance(obj, str) or not isinstance(obj, collections.abc.Iterable):
            obj = [obj]

        for o in obj:
            # can be NodeGroup, string, and NodeSet
            if isinstance(o, NodeGroup) or isinstance(o, NodeSet):
                self.contents.append(o)
            elif isinstance(o, str):
                for m in self.nodegroup_def_re.findall(o):
                    if m.startswith("@"):
                        # resolve nodegroup
                        self.contents.append(NodeGroup.registry[m[1:]])
                    else:
                        self.contents.append(NodeSet(m))

    @property
    def nodeset(self):
        # build and retrieve the (cached) NodeSet of this group.
        if self.nodeset_cache is None:
            n = NodeSet()
            for c in self.contents:
                if isinstance(c, NodeSet):
                    n += c
                elif isinstance(c, NodeGroup):
                    n += c.nodeset
            self.nodeset_cache = n
        return self.nodeset_cache

    @nodeset.setter
    def nodeset(self, value):
        # set the nodeset of this group. Careful with using this, 
        # the node contents and nodeset will go out of sync!
        if not isinstance(value, NodeSet):
            raise Exception("Expecting NodeSet instance")
        self.nodeset_cache = value

    @staticmethod
    def get(name):
        # Get a NodeGroup instance by name
        if name.startswith("@"):
            name = name[1:]
        if name in NodeGroup.registry:
            return NodeGroup.registry[name]
        raise Exception("Group does not exist!")

    def __str__(self):
        # print a NodeGroup
        ret = []
        for c in self.contents:
            if isinstance(c, NodeSet):
                ret.append(str(c))
            elif isinstance(c, NodeGroup):
                ret.append(f"@{c.name}")
        return ",".join(ret)

    def __repr__(self):
        # print a NodeGroup
        return f"<{self.__class__.__name__} {str(self)}>"

    def __bool__(self):
        # Check if the group has no nodes
        return bool(self.nodeset)

    def __nonzero__(self):
        # Check if the group has no nodes
        return self.__bool__()


class NodeSet(object):
    """
    A flat list of nodes (hostnames) with some formatting functionality.
    Underlying data structure is python's `set()`. Many operators are defined
    for `NodeSet()` in order to allow simple use of these objects.
    """
    number_re = re.compile(r'(.*[a-zA-Z])(\d+)(.*?)$')
    formatted_re = re.compile(r'([^\[]+)\[([\d\-,]+)\]')

    def __init__(self, obj=None):
        self.nodeset_cache = None
        self.content = None

        if obj is not None:
            self.set(obj)

    def set(self, obj=None):
        if isinstance(obj, set):
            self.nodeset_cache = obj
            self.content = None
        else:
            self.nodeset_cache = None
            self.content = obj

    @property
    def nodeset(self):
        if self.nodeset_cache is None:
            self.nodeset_cache = set()
            self.add(self.content)
        return self.nodeset_cache

    @nodeset.setter
    def nodeset(self, value):
        if not isinstance(value, set):
            raise Exception("Expecting set() instance")
        self.nodeset_cache = value

    def add(self, obj, remove=False):
        if isinstance(obj, str):
            for item in obj.split(","):
                if item.startswith("-"):
                    self.add(item[1:], remove=True)
                elif item.startswith("@"):
                    self.add(NodeGroup.get(item).nodeset, remove=remove)
                else:
                    prefixed_numbers = self.formatted_re.findall(item)
                    if prefixed_numbers:
                        fragments = []
                        for prefix, brackets in prefixed_numbers:
                            fragments.append([prefix])
                            numbers = []
                            for n in brackets.split(","):
                                if "-" in n:
                                    fr = n.split("-")
                                    num_digits = len(fr[0])
                                    n = range(int(fr[0]), int(fr[1]) + 1)
                                else:
                                    num_digits = len(n)
                                    n = [n]
                                numbers += n
                            fragments.append([str(x).zfill(num_digits) for x in numbers])
                        for node in itertools.product(*fragments):
                            nodename = "".join(node)
                            if remove:
                                self.nodeset.remove(nodename)
                            else:
                                self.nodeset.add(nodename)
                    else:
                        if remove:
                            self.nodeset.remove(item)
                        else:
                            self.nodeset.add(item)
        elif isinstance(obj, NodeSet):
            if remove:
                self.nodeset -= obj.nodeset
            else:
                self.nodeset |= obj.nodeset
            
        elif isinstance(obj, collections.abc.Iterable):
            for n in obj:
                self.add(n, remove=remove)
                
    def __len__(self):
        return len(self.nodeset)

    def __str__(self):
        return ",".join(self.formatted(self.nodeset))
    
    def __bool__(self):
        return bool(self.nodeset)
    
    def __nonzero__(self):
        return self.__bool__()

    def __repr__(self):
        return f"<{self.__class__.__name__} {str(self)}>"

    def __or__(self, other):
        return self.__add__(other)

    def __iter__(self):
        return self.nodeset.__iter__()

    def __and__(self, other):
        if isinstance(other, self.__class__):
            return NodeSet(self.nodeset & other.nodeset)
        if isinstance(other, str) or isinstance(other, collections.Iterable):
            return NodeSet(self.nodeset) & NodeSet(other)

        raise Exception("Operation not allowed")

    def __add__(self, other):
        if isinstance(other, self.__class__):
            return NodeSet(self.nodeset | other.nodeset)
        if isinstance(other, str) or isinstance(other, collections.Iterable):
            return NodeSet(self.nodeset) + NodeSet(other)

        raise Exception("Operation not allowed")

    def __sub__(self, other):
        if isinstance(other, self.__class__):
            return NodeSet(self.nodeset - other.nodeset)
        if isinstance(other, str) or isinstance(other, collections.Iterable):
            return NodeSet(self.nodeset) - NodeSet(other)

        raise Exception("Operation not allowed")

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.nodeset == other.nodeset
        if isinstance(other, str) or isinstance(other, collections.Iterable):
            return self.nodeset == NodeSet(other).nodeset
        return False

    def __contains__(self, key):
        return key in self.nodeset

    def formatted(self, inp, rec=False):
        buckets = {}
        res = []
        nodes = sorted(list(inp))
        did_concat = False
        for node in nodes:
            m = self.number_re.match(node)
            if not m:
                res.append(node)
                continue

            prefix, number, postfix = m.groups()
            numeric_len = len(number)

            buckets.setdefault((prefix, postfix, numeric_len), []).append(int(number))

        for (prefix, postfix, numeric_len), numbers in buckets.items():
            if len(numbers) == 1:
                res.append(f"{prefix}{str(numbers[0]).zfill(numeric_len)}{postfix}")
            else:
                did_concat = True
                formatted_numbers = self.get_line_numbers_concat(numbers, numeric_len)
                res.append(f"{prefix}[{formatted_numbers}]{postfix}")

        if did_concat:
            return self.formatted(res, rec=True)
        else:
            return res

    @staticmethod
    def get_line_numbers_concat(number_list, digits):
        # from https://stackoverflow.com/questions/29418693/write-ranges-of-numbers-with-dashes/29418827
        number_list = sorted(number_list)
        seq = []
        final = []
        last = 0

        for index, val in enumerate(number_list):

            if last + 1 == val or index == 0:
                seq.append(val)
                last = val
            else:
                if len(seq) > 1:
                    final.append(str(seq[0]).zfill(digits) + '-' + str(seq[len(seq) - 1]).zfill(digits))
                else:
                    final.append(str(seq[0]).zfill(digits))
                seq = []
                seq.append(val)
                last = val

            if index == len(number_list) - 1:
                if len(seq) > 1:
                    final.append(str(seq[0]).zfill(digits) + '-' + str(seq[len(seq) - 1]).zfill(digits))
                else:
                    final.append(str(seq[0]).zfill(digits))

        final_str = ','.join(map(str, final))
        return final_str


def read_config():
    """
    Reads the config file with node definitions and hierarchy.
    Default location is ~/.cash.topo.json
    @TODO: Make this a command line param
    """
    config_file = os.path.join(os.path.expanduser("~"), ".cash.topo.json")

    if not os.path.exists(config_file):
        config_file = os.path.join(ROOT, ".cash.topo.json")

    with open(config_file, 'r') as stream:
        return json.load(stream)


def build_sshtree(nodegroup, current_parent, set_taken=None, filter_=lambda nodegroup: nodegroup.nodeset):
    """
    This function builds a tree of nodes (using anytree's generic `Node()` class)
    out of the `NodeGroup()` hierarchy. The root `NodeGroup()` is in the `nodegroup` 
    parameter, and the current parent `Node()` is in `current_parent`. 
    `set_taken` is for internal use only, and with the `filter_` parameter a function
    to filter the `NodeGroup()` objects by `NodeSet()` can be specified. 

    This function works recursively. An example call would be like this:

        >> nodes_of_interest = NodeSet("@sc30")
        >> root = NodeGroup.get("all")
        >> root_node = Node('jumphost')
        >> build_sshtree(root, root_node, filter_=lambda nodegroup: nodegroup.nodeset & nodes_of_interest)  

    `root_node` then is a tree where each `node.name` is a hostname and it's children are also nodes. 
    """

    if not set_taken:
        set_taken = NodeSet()

    # Figure out the list of children of the current nodegroup, which are 
    # relevant given the `filter_()` function.
    filtered_children = []
    for c in nodegroup.children:
        if not filter_(c):
            continue
        else:
            filtered_children.append(c)

    # We harcode the following situation here: When the current node group has only
    # one child of interest, we skip this layer completely and immediately recurse 
    # into the function. This prevents single-node-chains like this:
    #    Node('/jumphost')
    #    └── Node('/jumphost/sc30n089')
    #        └── Node('/jumphost/sc30n089/sc30n090')
    #            └── Node('/jumphost/sc30n089/sc30n090/sc30n100')
    # @TODO: It may be favourable to keep these single-node-layers when a region
    # (norway, iceland, wolfsburg) is entered, as network may be slow between 
    # regions. Maybe implement this as an option.
    num_children = len(filtered_children)
    if num_children == 1:
        build_sshtree(filtered_children[0], current_parent, set_taken=set_taken, filter_=filter_)
    
    # recurse into the tree
    elif num_children > 1:
        for c in filtered_children:
            # generate a sorted list of nodes of this child and use the first
            # one as hostname for the current tree node. It is then added to 
            # set_taken, so it can be removed from the node list further down
            # in the tree.
            ll = sorted(list(filter_(c) - set_taken))
            if not ll:
                continue

            set_taken.add(ll[0])
            build_sshtree(c, Node(ll[0], parent=current_parent), set_taken=set_taken, filter_=filter_)
    
    # when this nodegroup has no children, we iterate its nodes and add them to the tree
    else:
        for n in sorted(list(filter_(nodegroup) - set_taken)):
            Node(n, parent=current_parent)


def rebalance(current_node, fansize=5):
    """
    Takes a node tree and rebalances it so that each level has at most `fansize` 
    children.
    """

    if len(current_node.children) > fansize:
        index_iter = list(range(fansize, len(current_node.children)))
        index_iter.reverse()
        for i in index_iter:
            current_node.children[i].parent = current_node.children[i % fansize]

    for c in current_node.children:
        rebalance(c, fansize=fansize)


def init():
    """
    Initialize topo Node groups from config file and build the topology
    `NodeGroup()` tree.
    """

    cfg = read_config()

    def build_tree(tree):
        children = []
        for name, val in tree.items():
            # Only the leafs (str) of the tree are actual nodesets; 
            # all others are groups of NodeSets. Because the Group's 
            # nodesets are evaluated lazily, we filter only on the 
            # the necessary.
            if isinstance(val, str):
                g = NodeGroup(name=name, contents=val)
            elif isinstance(val, dict):
                g = NodeGroup(name=name, contents=[f"@{x}" for x in val.keys()], children=build_tree(val))
            NodeGroup.registry[name] = g
            children.append(g)
        return children

    build_tree(cfg['nodes'])
    return NodeGroup.get("all")


def generate_setup_nodetree(exec_nodes=NodeSet(), fansize=50, flatten=False, jumphost=DEFAULT_JUMP_HOST):
    """
    Generates the `dict()` for the `node.py`'s `SetupMessage()` info datastructure. 
    """
    root = init()
    root_node = Node(jumphost)
    build_sshtree(root, root_node, filter_=lambda nodegroup: nodegroup.nodeset & exec_nodes)
    
    rebalance(root_node, fansize=fansize)

    def build_nodepy_setup_info(current_node, exec_nodeset, flatten=False):
        t = {}
        if not flatten:
            for c in current_node.children:
                t[c.name] = {
                    'tree': build_nodepy_setup_info(c, exec_nodeset),
                    'task': True
                }
        else:
            for c in current_node.descendants:
                t[c.name] = {
                    'tree': {},
                    'task': True
                }
        return t

    if jumphost:
        return {
            'task': False, 
            'tree': {
                root_node.name: {
                    'task': root_node.name in exec_nodes,
                    'tree': build_nodepy_setup_info(root_node, exec_nodes)
                }
            }
        }
    else:
        return {
            'task': False, 
            'tree': build_nodepy_setup_info(root_node, exec_nodes)
        }
