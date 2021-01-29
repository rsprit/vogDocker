import ssl
from typing import Any
from hypercorn.config import Config
import asyncio
import uvloop
from hypercorn.asyncio import serve
import sys
from os import path

# ToDo: very bad, that the file path is like this....
sys.path.append('../vogdb')
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from vogdb import main
import signal
import logging
import os

# logger
log = logging.getLogger(__name__)  # this logger works in any module

# Configuration
config = Config()
config.bind = ["localhost:8000"]  # here we add our domain
config.use_reloader = True
config.graceful_timeout = 60.0
config.h11_max_incomplete_size = 16384
config.h2_max_concurrent_streams = 100
config.h2_max_header_list_size = 65536
config.h2_max_inbound_frame_size = 16384
config.access_log_format = '%(h)s %(l)s %(l)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'
# ToDo: Add path to log files??
config.accesslog = "-"  # path to log file
config.errorlog = "-"  # path to error log file
config.statsd_host = "localhost:8000"

# config.server_names = ["test"]
# uncomment when we have an actual domain and SSL certificates
# config.ca_certs = <path/to/cert.pem>
# config.keyfile = <path/to/key.pem>
# config.insecure_bind = ["domain:80"]

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
shutdown_event = asyncio.Event()


def _signal_handler(*_: Any) -> None:
    shutdown_event.set()


def _exception_handler(loop, context):
    exception = context.get("exception")
    if isinstance(exception, ssl.SSLError):
        pass  # Handshake failure
    else:
        loop.default_exception_handler(context)


loop = asyncio.get_event_loop()

loop.add_signal_handler(signal.SIGTERM, _signal_handler)
loop.set_exception_handler(_exception_handler)
loop.run_until_complete(
    serve(main.api, config, shutdown_trigger=shutdown_event.wait)
)
