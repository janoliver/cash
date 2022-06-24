import argparse
import asyncio
import json
import signal

from cash import node, config, handlers, nodeset
from cash.nodeset import NodeSet, generate_setup_nodetree
from cash.utils import get_encoded_source


class Client:
    def __init__(self, nodes: NodeSet, node_plan: dict, message_handler: handlers.MessageHandler, ssh_timeout: int = 0):
        self.message_handler = message_handler
        self.nodes = nodes
        self.manager = node.SSHWorkerManager(
            self.message_handler, node_plan['tree'], encoded_source=get_encoded_source(), timeout=ssh_timeout
        )

    def stop(self):
        self.manager.cancel()
        self.message_handler.cancel()

    async def put(self, message: node.Message):
        await self.manager.send(message.pack())
        return self

    async def done(self):
        await self.put(node.ExitMessage())
        return self

    async def run(self):
        self.message_handler.setup(self.nodes)

        self.message_handler.start()
        self.manager.start()

        await self.manager.wait()
        self.message_handler.cancel()
        await self.message_handler.wait()


async def run(args):
    nodes = NodeSet(args.nodes)
    tree = generate_setup_nodetree(NodeSet(nodes), fansize=args.fansize, flatten=args.flatten, jumphost=args.jumphost)

    cmd_msg = node.CommandMessage(cmd=args.cmd.split() if not args.shell else args.cmd, shell=args.shell)

    handler = handlers.SingleCommandMessageHandler(cmd_msg,
                                                   output_format=args.out_format,
                                                   progress=args.progress,
                                                   hide0=args.hide0,
                                                   hide_empty=args.hide_empty)

    c = Client(nodes, tree, message_handler=handler, ssh_timeout=args.ssh_timeout)

    async def handler_ticker():
        while True:
            try:
                handler.tick()
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break

    t = asyncio.get_event_loop().create_task(handler_ticker())

    def kill():
        t.cancel()
        handler.kill()
        c.stop()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, kill)
    loop.add_signal_handler(signal.SIGTERM, kill)

    await c.put(cmd_msg)
    await c.done()
    await c.run()
    t.cancel()

    handler.tick(last=True)
    handler.print_summary()


async def plan(args):
    nodeset.init()

    nodes = NodeSet(args.nodes)

    root = nodeset.NodeGroup.get("@all")
    root_node = nodeset.Node(args.jumphost)

    nodeset.build_sshtree(root, root_node, filter_=lambda nodegroup: nodegroup.nodeset & nodes)
    nodeset.rebalance(root_node, fansize=args.fansize)

    t = generate_setup_nodetree(nodes, fansize=args.fansize, flatten=args.flatten, jumphost=args.jumphost)
    print(json.dumps(t))



def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("-n", "--nodes", metavar="NODES", type=str, default=config.DEFAULT_NODES_STRING,
                        help="Node or node groups.")
    parser.add_argument("--jumphost", metavar="JUMPHOST", type=str, default=config.DEFAULT_JUMP_HOST,
                        help="Gateway host to cluster.")
    parser.add_argument("--ssh-timeout", metavar="SSH_TIMEOUT", type=int, default=config.DEFAULT_SSH_TIMEOUT,
                        help="Define a timeout for SSH sessions. 0 = no timeout")
    parser.add_argument("-s", "--fansize", metavar="FANSIZE", type=int, default=config.DEFAULT_FANSIZE,
                        help="Maximum number of parallel SSH sessions.")
    parser.add_argument("--flatten", action="store_true" if config.DEFAULT_FLATTEN else "store_false",
                        help="Disable tree mode.")
    parser.add_argument("-p", "--progress", action="store_true", help="Show progress of received answers")

    g = parser.add_mutually_exclusive_group()
    g.add_argument('--json', dest='out_format', action='store_const', const='json', default=config.DEFAULT_OUT_FORMAT,
                   help='JSON output format')
    g.add_argument('--shell', dest='out_format', action='store_const', const='shell', default=config.DEFAULT_OUT_FORMAT,
                   help='Shell friendly output format')
    g.add_argument('--quiet', dest='out_format', action='store_const', const='quiet', default=config.DEFAULT_OUT_FORMAT,
                   help='No output')

    subparsers = parser.add_subparsers(help="Please use one of the following sub commands")

    # command runner
    parser_run = subparsers.add_parser("run", help="Run command")
    parser_run.set_defaults(func=run)
    parser_run.add_argument('-s', '--no-shell', dest="shell",
                            action='store_false' if config.DEFAULT_RUN_SHELL else "store_true",
                            help='Don\'t execute command in a shell')
    parser_run.add_argument('--hide0', action='store_true', help='Hide output with return code 0')
    parser_run.add_argument('--hide-empty', action='store_true', help='Hide nodes with empty output')
    parser_run.add_argument('cmd', metavar='COMMAND', type=str, help='command to execute')

    # plan
    parser_report = subparsers.add_parser("plan", help="Print tree as json to stdout (view with, e.g., firefox)")
    parser_report.set_defaults(func=plan)

    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(args.func(args))
    loop.close()


if __name__ == '__main__':
    main()
