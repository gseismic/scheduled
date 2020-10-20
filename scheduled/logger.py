import os
import sys
from logbook import Logger, StreamHandler, FileHandler, TimedRotatingFileHandler

"""
ref: 
    文档不准确，python3下，最新版本没有when
    https://docs.python.org/2/library/logging.handlers.html

True:
    https://logbook.readthedocs.io/en/stable/api/handlers.html#logbook.TimedRotatingFileHandler
"""

def get_logger(name="main", filename=None, 
               console=sys.stdout, 
               console_level='INFO',
               file_level='INFO', 
               by_date=True,
               bubble=True,
               date_format='%Y-%m-%d',
               rollover_format='{basename}{ext}.{timestamp}',
               **kwargs):

    console_level = console_level.upper()
    if console is not None:
        console_handler = StreamHandler(console, level=console_level)
        console_handler.push_application()
    if filename is not None:
        dirname = os.path.dirname(os.path.abspath(filename))
        if not os.path.exists(dirname):
            print('Create Log Dir: %s' % dirname)
            os.makedirs(dirname)
        if not by_date:
            handler = FileHandler(filename, bubble=True, level=file_level)
        else:
            handler = TimedRotatingFileHandler(filename, bubble=bubble,
                                               level=file_level, 
                                               date_format=date_format,
                                               rollover_format=rollover_format,
                                               **kwargs)
        handler.push_application()
    logger = Logger(name)
    return logger


if __name__ == "__main__":
    if 1:
        log = get_logger('lsl', filename='a.log')
        log.debug("logger self testing...")
        log.info("logger self testing...")
        log.notice("logger self testing...")
        log.warning("logger self testing...")
        log.error("logger self testing...")
