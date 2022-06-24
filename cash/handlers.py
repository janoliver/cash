#!/bin/env python

import datetime
import json
import time

try:
    from termcolor import cprint
except:
    def cprint(text, color=None, on_color=None, attrs=None, **kwargs):
        print(text, **kwargs)

from cash import node, config
from cash.nodeset import NodeSet
from cash.utils import print_progress


class MessageHandler(node.QueueingAsyncTask):
    """
    A class that defines some functions that are called by the `runner()`
    function in the event of incoming messages. Override this to
    customize behaviour. This `MessageHandler()` class is only an interface
    (does nothing...).
    """

    def __init__(self, *args, **kwargs):
        """self.nodes should contain the set of target nodes"""
        super().__init__(*args, unpack_messages=True, exit_gracefully=True, exit_before_handle=False, **kwargs)
        self.nodes = NodeSet()
        self.start_time = None
        self.end_time = None

    def kill(self):
        """Process has been killed."""
        self.end_time = time.time()

    def setup(self, target_nodes: NodeSet):
        """setup function, called by `runner()` with target nodes as argument"""
        self.nodes = target_nodes
        self.num_nodes = len(target_nodes)
        self.start_time = time.time()

    def finish(self):
        """finish function, called by `runner()`"""
        self.end_time = time.time()

    def is_finished(self) -> bool:
        return self.end_time is not None

    def handle(self, host: str, message: node.Message):
        """handle an incoming `Message` from `host`"""
        print(host, message.stdout)

    def exception(self, host: str, exception: node.ExceptionMessage):
        """handle an incoming `EcceptionMessage` from `host`"""
        pass

    async def handle_message(self, message: node.Message):
        if isinstance(message, node.ExceptionMessage):
            self.exception(message.host, message)
        elif isinstance(message, node.ExitMessage):
            self.finish()
        elif isinstance(message, node.Message):
            self.handle(message.host, message)


class ExceptionStoringMessageHandler(MessageHandler):
    """Handler that stores incoming exceptions and the set of hosts"""
    def __init__(self, *args, **kwargs):
        super(ExceptionStoringMessageHandler, self).__init__(*args, **kwargs)
        self.exceptions = {}
        self.exception_nodes = NodeSet()

    def exception(self, host, exception):
        self.exceptions[host] = exception
        self.exception_nodes.add(host)


class SingleCommandMessageHandler(ExceptionStoringMessageHandler):
    """Handler that expects a response of a single command from each
    host. Prints some progess in each tick and can print a summary"""
    def __init__(self,
                 command_message,
                 *args,
                 hide0=True,
                 hide_empty=False,
                 progress=False,
                 output_format=config.DEFAULT_OUT_FORMAT,
                 **kwargs):
        super(SingleCommandMessageHandler, self).__init__(*args, **kwargs)

        self.cmdmsg = command_message
        self.combine_results = True
        self.hide0 = hide0
        self.hide_empty = hide_empty
        self.results = {}
        self.successful_nodes = NodeSet()
        self.stats_timer = time.time()
        self.stats_timer_remaining = time.time()
        self.progress = progress
        self.of = output_format

    def kill(self):
        print("\n\n ### Caught CTRL+C! Hard interrupt. ###\n\n")

    def tick(self, last=False):
        if self.progress and self.of == 'text' and (last or time.time() - self.stats_timer > 0.1):
            self.stats_timer = time.time()
            print_progress(len(self.successful_nodes),
                           len(self.nodes),
                           suffix=f"responses and {len(self.exception_nodes)} exceptions received...",
                           stop=last)

        if self.of == 'text' and time.time() - self.stats_timer_remaining > 10.:
            self.stats_timer_remaining = time.time()
            unknown = self.nodes - self.exception_nodes - self.successful_nodes
            print(f"\nWaiting for remaining nodes {unknown} ...")

        if self.of == 'text' and last and self.progress:
            elapsed_seconds = str(datetime.timedelta(seconds=time.time() - self.start_time))
            print(f"\nFinished. Elapsed time: {elapsed_seconds} s\n")

    def handle(self, host, msg):
        if msg.id != self.cmdmsg.id:
            return

        self.successful_nodes.add(host)

        stdout = msg.stdout.strip()

        if self.hide_empty and not stdout:
            return

        if self.combine_results:
            self.results.setdefault(stdout, {'response': stdout, 'nodes': NodeSet()})['nodes'].add(host)
        else:
            self.results[host] = stdout

    def print_summary(self, hide_output=False):
        unknown = self.nodes - self.exception_nodes - self.successful_nodes

        if self.of == 'text':
            if self.successful_nodes:
                if self.combine_results:
                    for r in self.results.values():
                        cprint(f"[Success] {r['nodes']}", "green")
                        if not hide_output:
                            print(r['response'])
                else:
                    for host, r in self.results.items():
                        cprint(f"{host}: {r}", "green")

            if self.exceptions:
                for host, e in self.exceptions.items():
                    cprint(f"[Error] {host}: {e.message}", "red")
                    if e.traceback.strip():
                        print(e.traceback.strip())

            if unknown:
                cprint(f"[Unknown] {unknown}", "yellow")

        elif self.of == 'json':

            a = {
                'duration': str(datetime.timedelta(seconds=self.end_time - self.start_time)),
                'results': [],
                'errors': [],
                'unknown': []
            }

            if self.successful_nodes:
                for r in self.results.values():
                    for n in r['nodes']:
                        a['results'].append({'output': r['response'], 'node': n})

            if self.exceptions:
                for host, e in self.exceptions.items():
                    a['errors'].append({'message': e.message, 'node': host})

            if unknown:
                for n in unknown:
                    a['unknown'].append(n)

            print(json.dumps(a, indent=4))

        elif self.of == 'shell':
            if self.successful_nodes:
                for r in self.results.values():
                    for n in r['nodes']:
                        rn = r['response'].replace('\n', ';')
                        print(f"SUCCESS {n} {rn}")

            if self.exceptions:
                for host, e in self.exceptions.items():
                    print(f"ERROR {host} {e.message}")

            if unknown:
                for n in unknown:
                    print(f"UNKNOWN {n}")

    def received_all(self):
        return not bool(self.nodes - self.exception_nodes - self.successful_nodes)
