# encoding: utf8
import os
import sys
import logbook
import logbook.more
from logbook import *

import datetime
import threading


_logger_dict = {}
_pending_logger_config = {}


def get_logger2(name='main',
               filename=None,
               console = sys.stdout,
               by_date = True):
    pass


def get_logger(name="main", filename=None, 
               console=sys.stdout, 
               console_level='INFO',
               file_level='INFO', 
               by_date=True,
               bubble=True,
               date_format='%Y-%m-%d',
               rollover_format='{basename}{ext}.{timestamp}',
               file_mode='a',
               **kwargs):
    if name in _logger_dict:
        print('Loading ...')
        return _logger_dict[name]

    logbook.set_datetime_format('local')
    logger = Logger(name)
    # logger.handlers = []
    console_level = console_level.upper()
    file_level = file_level.upper()
    if console is not None:
        console_handler = StreamHandler(console, level=console_level)
        # console_handler = logbook.more.ColorizedStderrHandler(level=console_level)
        # logger.handlers.append(console_handler)
        console_handler.push_application()

    if filename is not None:
        dirname = os.path.dirname(os.path.abspath(filename))
        if not os.path.exists(dirname):
            print('Create Log Dir: %s' % dirname)
            os.makedirs(dirname)
        if not by_date:
            handler = FileHandler(filename, mode=file_mode, bubble=bubble,
                                  level=file_level.upper())
        else:
            print('file mode', file_mode)
            handler = TimedRotatingFileHandler(filename, bubble=bubble,
                                               mode=file_mode,
                                               level=file_level.upper(),
                                               date_format=date_format,
                                               rollover_format=rollover_format,
                                               **kwargs)
        handler.push_application()
        # logger.handlers.append(handler)
    # logger.handlers.append(handler)
    _logger_dict[name] = logger
    return logger


if __name__ == "__main__":
    if 1:
        log = get_logger('lsl', file_level='DEBUG', filename='a.log')
        log.debug("logger self testing...")
        log.info("logger self testing...")
        log.notice("logger self testing...")
        log.warning("logger self testing...")
        log.error("logger self testing...")
