import logging
import sys

# -------------------------------------------------------------------------------------
# Method to set logging information
def set_logging(log_file   = 'log.txt',
                log_level  = 'INFO',
                log_format = None):

    if log_format is None:
        log_format = '%(asctime)s %(name)-12s [%(lineno)-4s - %(funcName)10s()] %(levelname)-8s: %(message)s'

    level = logging.getLevelName(log_level)
    formatter = logging.Formatter(log_format)
    # # Remove old logging file
    # if os.path.exists(logger_file):
    #     os.remove(logger_file)

    # Set level of root debugger
    #logging.root.setLevel(logging.INFO)

    # Open logging basic configuration (for file logging)
    #logging.basicConfig(level=level, format=log_format, filename=log_file, filemode='a')

    # Set logger handlers (console and file) 
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

    logging.basicConfig(level = level, handlers=[file_handler, console_handler])
# -------------------------------------------------------------------------------------