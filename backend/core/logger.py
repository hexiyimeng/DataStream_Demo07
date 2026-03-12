import logging
import sys

# 配置格式
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging():
    """
    初始化全局日志配置
    """
    # 1. 基础配置
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # 2. 压制一些过于啰嗦的库日志
    logging.getLogger("distributed").setLevel(logging.WARNING)
    logging.getLogger("bokeh").setLevel(logging.WARNING)

    # 3. 返回主 Logger
    logger = logging.getLogger("BrainFlow")
    logger.setLevel(logging.INFO)
    return logger


# 创建单例 logger 供其他模块导入
logger = setup_logging()