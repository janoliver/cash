#!/bin/env python3

"""
This script is the message recipient, sender, forwarder, and
executor of the `cash` tool. It is supposed to run
without external dependencies (despite compatible Python versions
> 3.6) on the compute nodes that take part in a `cash` run.

This script is started via SSH on a compute node. Generally, it listens to
`stdin` for messages (subclasses of the `Message()` class), and writes its
output to `stdout`. If child SSH processes are spawned, it sends messages
to its children via their `stdin` streams and receives their messages via 
the corresponding `stdout` streams. 

First, this script expects a `SetupMessage()` (see below) 
with meta information and, most importantly, the subtree of compute nodes 
(children) that this instance serves. It then opens an SSH connection 
_calling itself_ on each node in the first level of the subtree. 
This kind of recursion/fan-out/nesting/cascading allows nested message 
passing to and through a large number of nodes. The topology of the tree 
should ideally match the network topology to most efficiently distribute 
network communication and workload.

The script handles concurrent IO with asyncio. All communication with this script
happens via `stdin` and `stdout` in binary mode. The communication
protocol is defined by the `Message()` class and its subclasses 
(see below).

When a message arrives on `stdin`, it is forwarded to the children
via the SSH connection's `stdin` pipe. When the message has an
execution part (i.e., some task attached to it) and the current 
node is among the task receivers, it executes the message's task.
Task execution results are (again, as a `Message()` instance) written
to `stdout`. Results coming in from  this script's children are forwarded
to `stdout`, so that the root process can collect all results from all
the nodes in the tree.

Ideally, a child process and all it's threads are terminated
when an `ExitMessage()` is received. The message is, as usual,
forwarded to this script's children and also to the execution thread,
which is terminated. When all children are terminated, the current
script also exits gracefully. 

If there is an error, an `ExceptionMessage()` is sent back if possible.
Please see the logs in that case.

We try hard not to leave zombie processes open on execution nodes. If 
a case can be constructed where this happens, please report the issue!
"""

import asyncio
import os
import signal
import sys
import struct
import random
import json
import logging
import traceback
from contextlib import suppress
from logging.handlers import RotatingFileHandler
import socket
import datetime
import collections

import typing

# set up logging
# CRITICAL (the default) is basically: log nothing.
log_level = os.getenv('CASH_LOG_LEVEL', 'CRITICAL')
log_file = os.getenv('CASH_LOG_FILE', '/tmp/cash.log')
this_host = os.getenv('CASH_HOST', socket.gethostname())
fromhost = os.getenv('CASH_FROMHOST', 'UNKNOWN')
ssh_timeout = float(os.getenv('CASH_TIMEOUT', '0'))

log_formatter = logging.Formatter('%(asctime)s LINE:%(lineno)d THREAD:%(threadName)s %(levelname)s %(message)s')
log_handler = RotatingFileHandler(log_file, mode='a', maxBytes=5*1024*1024, backupCount=1, encoding=None, delay=True)
logger = logging.getLogger()
logger.setLevel(log_level)
logger.addHandler(log_handler)
log_handler.setFormatter(log_formatter)
log_handler.setLevel(log_level)

# a simple map to register the available message classes
# with their `type_code` as key. When a message is received
# over a socket, one of the first bytes is this key and the
# correct message class can be constructed from the input 
# bytes. `message_registry` is filled by the Meta Class 
# `MessageMetaClass`.
message_registry = {}


def register_message(type_code=None):
    if not type_code or not type(type_code) == bytes:
        raise ValueError("Wrong type code")

    def decorator(cls):
        if type_code not in message_registry:
            message_registry[type_code] = cls
            cls.type_code = type_code
        return message_registry[type_code]
    return decorator


# CPU sleep timeout: in infinite loops use this timeout
# in every cycle, so that other threads can use the CPU as well
# This is due to Python's GIL
# in seconds
CPU_LOOP_TIMEOUT = 0.01


class EmptyReadException(Exception):
    """
    When a `read()` from a socket returns an empty byte string
    b'', the socket is likely broken. In that case this Exception
    is thrown and caught gracefullly. 
    """
    pass


class ConnectionBrokenError(RuntimeError):
    pass


class Field(object):
    """
    Base class of the data fields that can be associated with messages.
    This basically only provides `serialize()` and `deserialize()` methods
    to be overridden by the subclasses, and parameters of default value and
    whether the data field is included in inbound and/or outbound messages.
    """
    def __init__(self, inbound=False, outbound=False, default=None):
        self.value = default
        self.inbound = inbound
        self.outbound = outbound

    def serialize(self):
        return self.value

    def deserialize(self, byte_data):
        self.value = byte_data


class BinaryField(Field):
    pass


class TextField(Field):
    def serialize(self):
        return self.value.encode('utf-8')

    def deserialize(self, byte_data):
        self.value = byte_data.decode('utf-8')


class TimeField(Field):
    def serialize(self):
        return self.value.strftime("%Y-%m-%d %H:%M:%S").encode('utf-8')

    def deserialize(self, byte_data):
        self.value = datetime.datetime(
            int(byte_data[0:4]),
            int(byte_data[5:7]),
            int(byte_data[8:10]),
            int(byte_data[11:13]),
            int(byte_data[14:16]),
            int(byte_data[17:19]))


class JSONField(Field):
    def serialize(self):
        return json.dumps(self.value).encode('utf-8')

    def deserialize(self, byte_data):
        self.value = json.loads(byte_data.decode('utf-8'))


class IntField(Field):
    def serialize(self):
        return struct.pack(b'i', self.value)

    def deserialize(self, byte_data):
        self.value = struct.unpack(b'i', byte_data)[0]


class BooleanField(Field):
    def serialize(self):
        return struct.pack(b'?', self.value)

    def deserialize(self, byte_data):
        self.value = struct.unpack(b'?', byte_data)[0]


class Message(object):
    """
    Abstract Message class. This class has some methods to send
    and receive messages over sockets. Data fields to be sent/received 
    are defined as class attributes like `time` and `host` below.
    """

    # length of the ID that is generated for each message
    ID_LEN = 8

    # cache length of an int in bytes
    LEN_INT = struct.calcsize(b'i')

    responding = False  # Whether this class has a `task` attached to it.
    INBOUND = b's'  # code to indicate whether the message is inbound
    OUTBOUND = b'r'  # code to indicate whether the message is outbound
    START_CODE = b'PMSG'  # magic start code to indicate a new message.

    def __init__(self, is_response=False, msgid=None, uniqid=None, no_init=False, **kwargs):
        
        # generate dict holding the values of the `Field()s`.
        # One for each inbound and outbound
        self.__dict__['fields'] = collections.OrderedDict()
        
        self._kwargs = kwargs
        
        self.is_response = is_response

        # host and time are default outbound fields, so that the originating host
        # and time are always known for all outbound messages.
        if not no_init:
            self.init_fields()
            self.finish_fields()

        # if required, generate random IDs for the message. https://docs.python.org/3/howto/sockets.html
        # msg.id is an id that is shared between a message and it's responses,
        # whereas msg.uniqid is unique to every single instance of message.
        if msgid is None:
            msgid = self.id_generator(size=self.ID_LEN)

        if uniqid is None:
            uniqid = self.id_generator(size=self.ID_LEN)
            
        self.id = msgid
        self.uniqid = uniqid
        self.raw_data = b''

    def init_fields(self):
        self.fields['time'] = TimeField(inbound=False, outbound=True, default=datetime.datetime.now())
        self.fields['host'] = TextField(inbound=False, outbound=True, default=this_host)
        
    def finish_fields(self):
        for k, v in self.fields.items():
            if k in self._kwargs:
                v.value = self._kwargs.pop(k)

    @staticmethod
    def id_generator(size=ID_LEN, chars='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'):
        return ''.join(random.choice(chars) for _ in range(size)).encode('utf-8')

    def __getattr__(self, name):
        if name in self.fields:
            return self.fields[name].value
        
    def __setattr__(self, name, value):
        if name in self.fields:
            self.fields[name].value = value
            return

        return object.__setattr__(self, name, value)

    def pack(self):
        if self.raw_data:
            # already packed
            return self

        # determine field data to send and total size
        data = b''
        for field_name, field in self.fields.items():
            if (self.is_response and field.outbound) or (not self.is_response and field.inbound):
                field_data = field.serialize()
                data += struct.pack(b'i', len(field_data)) + field_data

        self.raw_data = data

        return self

    def unpack(self):
        if not self.raw_data:
            # already unpacked
            return self

        self.init_fields()

        cur_pos = 0

        for field_name, field in self.fields.items():
            if (self.is_response and field.outbound) or (not self.is_response and field.inbound):
                field_len, = struct.unpack(b'i', self.raw_data[cur_pos:cur_pos+self.LEN_INT])
                cur_pos += self.LEN_INT
                field.deserialize(self.raw_data[cur_pos:cur_pos+field_len])
                cur_pos += field_len

        #self.raw_data = b''
        return self

    @staticmethod
    async def fromsock(sock):
        """
        Construct a `Message()` object from a socket. The first byte
        determines whether the message is incoming (i.e., travelling 
        _to_ children) or outgoing (travelling to the parent). 
        The second byte determines the type of Message. A unique
        ID of each message is then communicated. Then, the remaining payload
        is read.
        """

        id_len = Message.ID_LEN
        len_int = Message.LEN_INT

        len_start = len(Message.START_CODE)
        num_read = len_start+1+1+id_len+id_len+len_int
        buff = await sock.readexactly(num_read)

        code = buff[0:len_start]
        direction = buff[len_start:len_start+1]
        msg_type = buff[len_start+1:len_start+2]
        msgid = buff[len_start+2:len_start+2+id_len]
        uniqid = buff[len_start+2+id_len:len_start+2+id_len+id_len]
        raw = buff[len_start+2+id_len+id_len:len_start+2+id_len+id_len+len_int]

        if code != Message.START_CODE:
            raise ConnectionBrokenError("message code not correct.")

        cls = message_registry[msg_type]

        # determine total size of payload
        msg_len, = struct.unpack(b'i', raw)
        raw_data = await sock.readexactly(msg_len)
        
        msg = cls(msgid=msgid, uniqid=uniqid, no_init=True)
        msg.is_response = (direction == Message.OUTBOUND)
        msg.raw_data = raw_data

        return msg

    async def tosock(self, sock):
        """
        Send an message to a socket by serializing in-/outgoing 
        type, Message type and message ID. 
        """ 
        sock.write(Message.START_CODE)
        sock.write(Message.OUTBOUND if self.is_response else Message.INBOUND)
        sock.write(self.type_code)
        sock.write(self.id)
        sock.write(self.uniqid)
        sock.write(struct.pack(b'i', len(self.raw_data)))
        sock.write(self.raw_data)
        await sock.drain()

    async def respond(self, **kwargs):
        """
        Generate a response. A response is a `Message()` instance
        with `is_response=True` and some meta data set. This should
        be overridden when additional data is to be attached to the 
        response.
        """
        msg = self.__class__(is_response=True, msgid=self.id, **kwargs)

        for k, v in self.fields.items():
            msg.fields[k].value = kwargs.get(k, v.value)

        return msg

    async def execute(self):
        pass

    def __repr__(self):
        return "<%s: %s %s>" % (self.__class__.__name__, self.id, self.uniqid)


class OutgoingMessage(Message):
    """
    Abstract class for outgoing only messages, setting is_responding=True
    """
    responding = True

    def __init__(self, *args, **kwargs):
        kwargs['is_response'] = kwargs.get('is_response', True)
        super(OutgoingMessage, self).__init__(*args, **kwargs)


@register_message(type_code=b'S')
class SetupMessage(Message):
    """
    Message carrying information for this script to setup it's threads
    and children processes.
    """

    def init_fields(self):
        super(SetupMessage, self).init_fields()
        self.fields['setup_info'] = JSONField(inbound=True)


@register_message(type_code=b'J')
class CommandMessage(Message):
    """
    A message to execute a shell command on the nodes and send
    back the result.
    """
    responding = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.proc = None

    def init_fields(self):
        super(CommandMessage, self).init_fields()
        self.fields['shell'] = BooleanField(inbound=True, default=False)
        self.fields['sort'] = BooleanField(inbound=True, default=False)
        self.fields['cmd'] = JSONField(inbound=True)
        self.fields['stdout'] = TextField(outbound=True)
        self.fields['stderr'] = TextField(outbound=True)
        self.fields['result_code'] = IntField(outbound=True)

    async def respond(self, **kwargs):
        msg = await super(CommandMessage, self).respond(**kwargs)
        if self.shell:
            self.proc = await asyncio.create_subprocess_shell(
                self.cmd,
                loop=asyncio.get_event_loop(),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
        else:
            self.proc = await asyncio.create_subprocess_exec(
                *self.cmd,
                loop=asyncio.get_event_loop(),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
        out, err = await self.proc.communicate()

        if self.sort:
            out = b"\n".join(sorted(b"\n".split(out)))

        msg.stdout = out.decode('utf-8')
        msg.stderr = err.decode('utf-8')
        msg.result_code = self.proc.returncode

        self.proc = None

        return msg

    def cancel(self):
        if self.proc:
            logging.debug("Killing process %s", self.cmd)
            self.proc.kill()


@register_message(type_code=b'E')
class ExceptionMessage(OutgoingMessage):
    # A message that carries an exception to be returned to the parents.
    responding = True

    def init_fields(self):
        super(ExceptionMessage, self).init_fields()
        self.fields['message'] = TextField(outbound=True, default='Undefined error')
        self.fields['traceback'] = TextField(outbound=True, default='')


@register_message(type_code=b'X')
class ExitMessage(Message):
    # A message that signals the processes to exit.
    pass


@register_message(type_code=b'A')
class ReadyMessage(OutgoingMessage):
    # A message that signals that this process is now ready.
    pass


class AsyncTask:
    """
    A stoppable thread (via `t.stoprequest`) that provides a
    thread-owned incoming task queue and writes to a shared
    `out_queue`.
    """
    def __init__(self, exit_gracefully=True, exit_before_handle=True, *args, **kwargs):
        self.task = None
        self.exit_gracefully = exit_gracefully
        self.exit_before_handle = exit_before_handle

    async def run(self):
        while True:
            try:
                res = await self.get_message()
                logging.debug("%s: Received message %s", self.__class__.__name__, res)

                if self.exit_gracefully and self.exit_before_handle and isinstance(res, ExitMessage):
                    logging.debug("Exiting ConnectorAsyncTask")
                    return

                await self.handle_message(res)

                if self.exit_gracefully and not self.exit_before_handle and isinstance(res, ExitMessage):
                    logging.debug("Exiting ConnectorAsyncTask")
                    return

            except asyncio.CancelledError as e:
                logging.debug("%s: Cancelling ConnectorAsyncTask", self.__class__.__name__)
                await self.handle_exception(e)
                return

            except Exception as e:
                logging.exception("%s: ERROR forwarding data", self.__class__.__name__)
                await self.handle_exception(e)
                return

    async def get_message(self) -> Message:
        ...

    async def handle_message(self, message: Message):
        ...

    async def handle_exception(self, exception: Exception = None):
        ...

    def start(self, loop=None):
        loop = loop or asyncio.get_event_loop()
        self.task = loop.create_task(self.run())
        return self

    def cancel(self):
        self.task.cancel()

    def cancelled(self) -> bool:
        return self.task.cancelled()

    def done(self):
        return self.task.done()

    async def wait(self):
        await self.task


class QueueingAsyncTask(AsyncTask):
    def __init__(self, unpack_messages=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_queue = asyncio.Queue()
        self.unpack_messages = unpack_messages

    async def send(self, message):
        await self.task_queue.put(message)

    async def get_message(self) -> Message:
        message = await self.task_queue.get()
        self.task_queue.task_done()
        if self.unpack_messages:
            message.unpack()
        return message


class ConnectorAsyncTask(AsyncTask):
    """
    Task that connects a pair of queues or sockets, in either direction.
    Graceful exit on ExitMessage receive.
    """
    def __init__(self,
                 inp: typing.Union[asyncio.Queue, asyncio.StreamReader],
                 output: typing.Union[asyncio.Queue, asyncio.StreamWriter],
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inp = inp
        self.output = output

    async def get_message(self) -> Message:
        if isinstance(self.inp, asyncio.Queue):
            msg = await self.inp.get()
            self.inp.task_done()
            return msg
        elif isinstance(self.inp, asyncio.StreamReader):
            return await Message.fromsock(self.inp)

    async def handle_message(self, msg: Message):
        if isinstance(self.output, asyncio.Queue):
            await self.output.put(msg)
        elif isinstance(self.output, asyncio.StreamWriter):
            await msg.tosock(self.output)


class CollectorTask(QueueingAsyncTask):
    """
    Thread that reads from the `out_queue` and writes to a socket. This thread forwards
    all messages from the children to the parent.
    No graceful exit strategy.
    """
    def __init__(self, out_sock: asyncio.StreamWriter, *args, **kwargs):
        super().__init__(*args, exit_gracefully=True, exit_before_handle=False, **kwargs)
        self.sock = out_sock

    async def handle_message(self, message: Message):
        await message.tosock(self.sock)


class CommunicatingAsyncTask(QueueingAsyncTask):
    """
    A QueueingAsyncTask that writes some kind of response to a collector
    (CollectorTask).
    """
    def __init__(self, *args, collector: CollectorTask = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.collector = collector

    async def handle_message(self, message: Message):
        await self.collector.send(message)


class LocalWorkerThread(CommunicatingAsyncTask):
    """
    Thread that carries out local tasks, generates a message response and puts it to the collector.
    Graceful Exit on ExitMessage receive.
    """

    def __init__(self, host: str, *args, **kwargs):
        super().__init__(*args, unpack_messages=True, **kwargs)
        self.host = host
        self.current_message = None

    async def handle_message(self, message: Message):
        self.current_message = message
        await message.execute()
        logging.debug("%s was executed %s.", message, message.cmd)

        if message.responding:
            msg = await message.respond(host=self.host, time=datetime.datetime.now())
            logging.debug("%s was responded.", msg)
            await self.collector.send(msg.pack())

    async def handle_exception(self, exception: Exception = None):
        if self.current_message and isinstance(self.current_message, CommandMessage):
            self.current_message.cancel()

    def cancel(self):
        if self.current_message and isinstance(self.current_message, CommandMessage):
            self.current_message.cancel()
        super().cancel()


class SSHWorkerThread(CommunicatingAsyncTask):
    """
    Thread that opens an SSH connection to a child, forwards
    incoming messages to the child and receives outgoing message
    from it. The received messages are send to the collector.
    """

    def __init__(self, host: str, subtree: dict, encoded_source: str, timeout: float = 0,
                 fromhost: str = fromhost, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host
        self.subtree = subtree
        self.timeout = timeout
        self.fromhost = fromhost
        self.encoded_source = encoded_source

    async def try_next_host(self):
        """"
        When connection to self.host fails, the next host from the subtree is used as self.host.
        The remaining subtree is kept as-is. This avoids losing results from all hosts in the current
        subtree, when self.host is offline or erroneous.
        """
        self.host = list(self.subtree["tree"].keys())[0]
        self.subtree["task"] = self.subtree["tree"][self.host]["task"]
        self.subtree["tree"].update(self.subtree["tree"][self.host]["tree"])
        self.subtree["tree"].pop(self.host)
        await self.run()

    async def exception(self, message: str, exc: Exception = None) -> str:
        exc_str = traceback.format_exc(exc) if exc is not None else ""
        await self.collector.send(ExceptionMessage(message=message, traceback=exc_str, host=self.host).pack())
        return message

    async def run(self):
        logging.debug("Opening SSH connection to %s", self.host)

        cmd = [
            'ssh', '-x', self.host,
            '-o', 'PreferredAuthentications=publickey',
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'BatchMode=yes',
            f'CASH_LOG_FILE={log_file}',
            f'CASH_LOG_LEVEL={log_level}',
            f'CASH_HOST={self.host}',
            f'CASH_FROMHOST={this_host}',
        ]

        if self.timeout is not None:
            cmd += [f'CASH_TIMEOUT={self.timeout}']

        cmd += [f"/usr/bin/python3 -u -c 'import codecs,os,sys;_=codecs.decode;PPSRC=\"{self.encoded_source}\";"
                "exec(_(_(PPSRC.encode(),\"base64\"),\"zip\"))'"]

        p = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        logging.debug(cmd)

        # first, listen for ReadyMessage. When a timeout is set, and 
        # no message can be retrieved, fail with an error.

        try:
            message = await asyncio.wait_for(Message.fromsock(p.stdout), self.timeout or None)
            message.unpack()
            if isinstance(message, ReadyMessage):
                logging.debug("Received Ready message from %s: %s", self.host, message)

        except asyncio.TimeoutError:
            logging.exception(await self.exception("SSH timeout: %s -> %s" % (self.fromhost, self.host)))
            p.kill()
            await p.wait()
            await self.try_next_host()
            return

        except Exception as e:
            logging.exception(await self.exception("Could not connect to %s: %s" % (self.host, str(e)), exc=e))
            # recover by using next host
            p.kill()
            await p.wait()
            await self.try_next_host()
            return

        # send tree message
        try:
            msg = SetupMessage(setup_info=self.subtree).pack()
            await msg.tosock(p.stdin)
            logging.debug("Successfully sent %s", msg)

        except Exception as e:
            logging.exception(await self.exception("Couldn't send %s to %s: %s" % (message, self.host, str(e)), exc=e))
            p.kill()
            await p.wait()
            await self.try_next_host()
            return

        result_task = ConnectorAsyncTask(p.stdout, self.collector.task_queue).start()
        forwarder_task = ConnectorAsyncTask(self.task_queue, p.stdin, exit_before_handle=False).start()
        await asyncio.wait([result_task.task, forwarder_task.task])


class SSHWorkerManager:
    def __init__(self, collector: QueueingAsyncTask, children_tree: dict, encoded_source: str = None, timeout: float = None):
        self.tasks = []

        for node, subtree in children_tree.items():
            logging.debug("Opening connection to %s with subtree %s", node, subtree)
            self.tasks.append(SSHWorkerThread(
                node,
                subtree,
                encoded_source or PPSRC,
                collector=collector,
                timeout=timeout
            ))
    
    def start(self):
        for t in self.tasks:
            t.start()

    def cancel(self):
        for t in self.tasks:
            t.cancel()

    async def send(self, message):
        if self.tasks:
            await asyncio.wait(map(lambda x: x.send(message), self.tasks))

    async def wait(self):
        if self.tasks:
            await asyncio.wait([t.task for t in self.tasks])

    def done(self):
        return all(map(lambda x: x.done(), self.tasks))


class ForwarderTask(AsyncTask):
    """
    Thread that reads from the `out_queue` and writes to
    a socket. This thread forwards all messages from
    the children to the parent.
    """
    def __init__(self, in_sock: asyncio.StreamReader, *args, **kwargs):
        super().__init__(*args, exit_gracefully=True, exit_before_handle=False, **kwargs)
        self.sock = in_sock
        self.tasks = []

    def add_receiver(self, task: typing.Union[QueueingAsyncTask, SSHWorkerManager]):
        self.tasks.append(task)

    async def handle_message(self, message: Message):
        await asyncio.wait(map(lambda x: x.send(message), self.tasks))

    async def get_message(self) -> Message:
        return await Message.fromsock(self.sock)

    def handle_exception(self, exception: Exception = None):
        if exception and isinstance(exception, ConnectionResetError):
            logging.debug("Connection to parent lost, exiting.")
            self.cancel()


async def main():
    loop = asyncio.get_event_loop()

    # The inbound messages coming from stdin. Create a StreamReader for convenience.
    insock = asyncio.StreamReader()
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(insock), sys.stdin)

    # The outbound messages going to stdout. Create a StreamWriter for convenience.
    w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout)
    outsock = asyncio.StreamWriter(w_transport, w_protocol, None, loop)

    async def monitor_stdin_eof():
        while True:
            await asyncio.sleep(0.1)
            if insock.at_eof():
                await die()
    loop.create_task(monitor_stdin_eof())

    try:
        # The collector holds a queue that can receive messages
        # and sends those messages to stdout socket. This is the `backchannel`.
        collector = CollectorTask(outsock).start()

        # signal ready state.
        await collector.send(ReadyMessage().pack())

        # First off, read a single message from stdin that contains information about
        # this instance of node.py and it's children. This has to be done before
        # `ForwarderTask` is started!
        logging.debug("Receiving setup")
        setup_message = (await SetupMessage.fromsock(insock)).unpack()
        is_task, tree = setup_message.setup_info['task'], setup_message.setup_info['tree']

        # The forwarder reads messages from stdin and forwards them to one or more
        # QueueingAsyncTasks (where a send() method is available)
        forwarder = ForwarderTask(insock).start()

        # The worker_manager orchestrates SSH connections to all the child nodes
        # The AsyncTasks are stored as a list in manager.tasks.
        worker_manager = SSHWorkerManager(collector, tree, timeout=ssh_timeout)
        worker_manager.start()

        # Forward incoming messages to the workers
        forwarder.add_receiver(worker_manager)

        # If the current host is a worker itself, i.e., has to carry out any local work,
        # start the worker thread and also connect it to the forwarder
        worker_thread = LocalWorkerThread(this_host, collector=collector)
        if is_task:
            forwarder.add_receiver(worker_thread)
            worker_thread.start()

        logging.debug("All tasks are started")

        await forwarder.task
        if forwarder.cancelled():
            # This is most likely due to the parent process exiting, so we arrange everything
            # else to quit, too.
            worker_manager.cancel()
            if is_task:
                worker_thread.cancel()

        await asyncio.wait([worker_manager.wait()] + ([worker_thread.task] if is_task else []))

        logging.debug("Trying to send exit message to socket...")
        await collector.send(ExitMessage().pack())
        await collector.task
        logging.debug("Graceful quit")

    except:
        # still attempt to send the exception back
        logging.exception("undefined...")
        sys.exit(1)


async def die():
    logging.debug("Received kill signal or stdin closed unexpectedly. Exiting!")
    tasks = [task for task in asyncio.Task.all_tasks() if task is not asyncio.tasks.Task.current_task()]
    for task in tasks:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    asyncio.get_event_loop().stop()
    sys.exit(1)


if __name__ == '__main__':
    logging.debug("Starting node.py on %s.", this_host)
    event_loop = asyncio.get_event_loop()

    event_loop.add_signal_handler(signal.SIGINT, lambda: asyncio.ensure_future(die()))
    event_loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.ensure_future(die()))

    event_loop.run_until_complete(main())
