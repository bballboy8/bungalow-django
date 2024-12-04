import sys
from loguru import logger
import copy
from starlette.requests import Request
from starlette.responses import Response
import datetime

def logging_api_requests(request: Request, response: Response):
    logs = f"{request.client.host}:{request.client.port} {request.method} {request.url} {response.status_code}"
    logs += "\n\n" + "*********Request Headers Start***********\n"
    logs += "\n".join(f"{name} : {value}" for name, value in request.headers.items())
    logs += "\n" + "*********Request Headers End***********\n"
    logs += "\n\n" + "*********Response Headers Start***********\n"
    logs += "\n".join(f"{name} : {value}" for name, value in response.headers.items())
    logs += "\n" + "*********Response Headers End***********\n"
    logger.debug(logs)

class Rotator:

    def __init__(self, *, size, at):
        now = datetime.datetime.now()

        self._size_limit = size
        self._time_limit = now.replace(hour=at.hour, minute=at.minute, second=at.second)

        if now >= self._time_limit:
            # The current time is already past the target time so it would rotate already.
            # Add one day to prevent an immediate rotation.
            self._time_limit += datetime.timedelta(days=1)

    def should_rotate(self, message, file):
        file.seek(0, 2)
        if file.tell() + len(message) > self._size_limit:
            return True
        if message.record["time"].timestamp() > self._time_limit.timestamp():
            self._time_limit += datetime.timedelta(days=1)
            return True
        return False

rotator = Rotator(size=10000000, at=datetime.time(0, 0, 0))   
separator = "\n------------------------------------------------------------------------------------------------------\n"
fmt = "{level} | {time:ddd MMMM YYYY, HH:mm:ss:SSS} |{file.path}| {name}:{function}:{line}" + separator + "{message}" + separator
logger.remove()
logger = copy.deepcopy(logger)     
logger = logger.opt(colors=True)
logger.add(sys.stdout, colorize=True, level="DEBUG", format=fmt, enqueue=True, backtrace=True)
logger.enable("logger")
logger.add("./logs/enablematch_server.log", colorize=True, rotation=rotator.should_rotate, level="DEBUG", format=fmt, enqueue=True, backtrace=True)
logger.add(sys.stderr, colorize=True, level="ERROR", format=fmt, enqueue=True, backtrace=True)
