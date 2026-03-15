import logging
import logging.handlers
import sys
import os

def setup_logger():
    # 使用基于脚本/可执行文件所在目录的绝对路径
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    log_dir = os.path.join(base_dir, 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    logger = logging.getLogger('LightingControl')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        # 使用 RotatingFileHandler 防止日志文件无限增长（最大5MB × 3备份）
        log_file = os.path.join(log_dir, 'app.log')
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, encoding='utf-8', maxBytes=5*1024*1024, backupCount=3
        )
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(filename)s:%(lineno)d - %(message)s')
        file_handler.setFormatter(file_formatter)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
        console_handler.setFormatter(console_formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
    return logger

global_logger = setup_logger()
