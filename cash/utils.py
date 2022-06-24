#!/bin/env python
# -*- coding: utf-8 -*-
import codecs
import errno
import fnmatch
import hashlib
import math
import os
import sys
import zlib
from distutils.util import strtobool
import subprocess as sp

try:
    input = raw_input
except NameError:
    pass

ROOT = os.path.abspath(os.path.dirname(__file__))

def rglob(path, pattern="*"):
    """
    Recursive walk through dir and return all files matching `pattern`
    """
    ret = []
    for root, _, filenames in os.walk(path):
        for filename in fnmatch.filter(filenames, pattern):
            ret.append(os.path.join(root, filename))
    return ret

def mkdir_p(path):
    """
    Create directories and allow existing 
    """
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def print_progress(iteration, total, prefix='', suffix='', stop=False):
    """
    Progress printing
    """
    length = 40
    full = math.floor(length * iteration / float(total))
    empty = length - full - 1

    print(f"{prefix}[{'-' * full}>{' ' * empty}] ({iteration}/{total}){suffix}", end="\r" if iteration < total and not stop else "\n")


def md5hash(data):
    m = hashlib.md5()
    m.update(data)
    return m.digest()


def parse_size(inp_str):
    if inp_str.endswith(('K','k')):
        return int(float(inp_str[:-1]) * 1024**1)
    elif inp_str.endswith('M'):
        return int(float(inp_str[:-1]) * 1024**2)
    elif inp_str.endswith('G'):
        return int(float(inp_str[:-1]) * 1024**3)
    else:
        return int(inp_str)


def query_yes_no(question):
    while True:
        sys.stdout.write(question + " [y/n] ")
        choice = input().lower()
        try:
            return strtobool(choice)
        except ValueError:
            sys.stdout.write("Please respond with 'y' or 'n'\n")


def get_encoded_source():
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "node.py")) as source_file:
        source = source_file.read()
        compressed = zlib.compress(source.encode(), 9)
        encoded = codecs.encode(compressed, 'base64').replace(b'\n', b'')
        return encoded.decode("utf-8")


def run_cmd(cmd, shell=False, cwd=None):
    if not shell and isinstance(cmd, str):
        cmd = " ".split(cmd)
    elif shell and not isinstance(cmd, str):
        cmd = " ".join(cmd)

    p = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE, shell=shell, cwd=cwd)
    output, error = p.communicate()

    return output.decode('utf-8'), error.decode('utf-8'), p.returncode
