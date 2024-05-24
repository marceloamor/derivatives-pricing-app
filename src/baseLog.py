import logging

logger = logging.getLogger

import logging.handlers
from datetime import datetime

# remote box to send logs
ip_address = "10.1.41.66"

print("Started at: " + str(datetime.now()))


def baseLog(name):
    # set up logging
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logger.debug)
    socketHandler = logging.handlers.SocketHandler(
        ip_address, logging.handlers.DEFAULT_TCP_LOGGING_PORT
    )
    # don't bother with a formatter, since a socket handler sends the event as
    # an unformatted pickle
    rootLogger.addHandler(socketHandler)

    # Now, we can log to the root logger, or any other logger. First the root...
    logger.info(name + "logger started at: " + str(datetime.now()))
    # build  different handlers
    # calcLog = logging.getLogger('lmeEngine.calc')

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logger.error)
    rootLogger.addHandler(ch)

    return rootLogger
