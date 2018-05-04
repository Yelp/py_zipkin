# -*- coding: utf-8 -*-
import socket

import clog
import staticconf


ZIPKIN_CONFIG_NAMESPACE = 'zipkin'
ZIPKIN_CONFIG_FILE = '/nail/srv/configs/zipkin.yaml'
ZIPKIN_CONFIG_MIN_RELOAD_INTERVAL = 5

ZIPKIN_FIREHOSE_IP = '169.254.255.254'
ZIPKIN_FIREHOSE_PORT = 20182

ZIPKIN_DEFAULT_FIREHOSE_ENABLED = False


class FirehoseHandler(object):
    """A configurable transport handler for firehose mode (CEP935)

    This handler implements truth value testing so that it can be enabled,
    disabled, or removed, while keeping the py_zipkin logic simple.

    When called with a thrift-encoded zipkin message (typically a
    thrift-encoded list of spans), it will forward this message over UDP to a
    proxy, which generally runs as a daemon on the local host.
    """

    def __init__(self):
        self.zipkin_namespace = staticconf.NamespaceReaders(
            ZIPKIN_CONFIG_NAMESPACE)
        self.config_watcher = staticconf.ConfigFacade.load(
            ZIPKIN_CONFIG_FILE,
            ZIPKIN_CONFIG_NAMESPACE,
            staticconf.YamlConfiguration,
            min_interval=ZIPKIN_CONFIG_MIN_RELOAD_INTERVAL,
        )
        self.firehose_socket = socket.socket(socket.AF_INET,  # Internet
                                             socket.SOCK_DGRAM)  # UDP

    def __bool__(self):
        # For python 3
        return self.is_enabled()

    def __nonzero__(self):
        # For python 2
        return self.is_enabled()

    def is_enabled(self):
        self.config_watcher.reload_if_changed()
        firehose_enabled = self.zipkin_namespace.read_bool(
            'enable_firehose',
            default=ZIPKIN_DEFAULT_FIREHOSE_ENABLED,
        )
        return firehose_enabled

    def __call__(self, message):
        try:
            self.firehose_socket.sendto(
                message,
                (
                    ZIPKIN_FIREHOSE_IP,
                    ZIPKIN_FIREHOSE_PORT,
                ),
            )
        except Exception as e:
            clog.log_line(
                'tmp_zipkin_error',
                'yelp_pyramid FirehoseHandler error: {}'.format(repr(e))
            )
