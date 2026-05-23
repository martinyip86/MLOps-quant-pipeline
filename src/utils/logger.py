from logging.handlers import RotatingFileHandler
import logging
import sys
import os

def setup_logger(name,log_file=None,fmt=None,clear_on_start=False):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    fmt = '%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s' if fmt is None else fmt

    formatter = logging.Formatter(
        fmt=fmt,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    if logger.handlers:
        has_file_hander = any(isinstance(h,logging.FileHandler) for h in logger.handlers)
        if log_file and not has_file_hander:
            _add_file_handle(logger,log_file,formatter,clear_on_start)
            return logger

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        _add_file_handle(logger,log_file,formatter,clear_on_start)

    return logger

def _add_file_handle(logger,log_file,formatter,clear_on_start):
    try:
        os.makedirs(os.path.dirname(log_file),exist_ok=True)

        file_mode = 'w' if clear_on_start else 'a'

        file_handler = RotatingFileHandler(log_file,mode=file_mode,maxBytes=10*1024*1024,backupCount=5,encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"🚨 [FATAL] Failed to set up file logging: {e}")    
        sys.exit()        
