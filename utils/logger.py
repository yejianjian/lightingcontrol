import logging
import logging.handlers
import sys
import os

def setup_logger():
    if not os.path.exists('logs'):
        os.makedirs('logs')
        
    logger = logging.getLogger('LightingControl')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        # 使用 RotatingFileHandler 防止日志文件无限增长（最大5MB × 3备份）
        file_handler = logging.handlers.RotatingFileHandler(
            'logs/app.log', encoding='utf-8', maxBytes=5*1024*1024, backupCount=3
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
