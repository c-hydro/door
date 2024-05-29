import logging
import os

# -------------------------------------------------------------------------------------
# Method to set logging information
def set_logging(log_file   = 'log.txt',
                log_level  = 'INFO',
                log_format = None,
                console    = False):

    if log_format is None:
        log_format = '%(asctime)s %(name)-12s [%(lineno)-4s - %(funcName)10s()] %(levelname)-8s: %(message)s'

    level = logging.getLevelName(log_level)
    formatter = logging.Formatter(log_format)

    # Set logger handlers (console and file) 
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    file_handler = logging.FileHandler(log_file, 'a')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # # create a root logger
    # root_logger = logging.getLogger()

    # # # Add handle to logging
    # root_logger.addHandler(file_handler)
    # root_logger.addHandler(console_handler)

    if console:
        logging.basicConfig(level = level, handlers=[file_handler, console_handler])
    else:
        logging.basicConfig(level = level, handlers=[file_handler])
# -------------------------------------------------------------------------------------