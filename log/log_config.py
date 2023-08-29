import logging, os

def log_config(log_file,logger_name='CentralLogger'):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO) # Set logger level to info
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
    
    print(f"Current Handlers: {logger.handlers}") # DEBUG
    
    # Get the directory where this log_config.py file is located
    log_dir = os.path.dirname(os.path.abspath(__file__))
    full_log_path = os.path.join(log_dir, log_file)
    print(f"Log file will be saved at: {full_log_path}") # DEBUG
    
    # Check if a FileHandler already exists, if not, create one
    if not any(isinstance(handler, logging.FileHandler) for handler in logger.handlers):
        # Create a handler to write logs to a file
        file_handler = logging.FileHandler(full_log_path)
        file_handler.setLevel(logging.INFO)  # Sets the handler's level to INFO

        # Define the log format
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        # Add the file handler to the logger
        logger.addHandler(file_handler)
    
    return logger