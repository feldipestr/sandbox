import logging
from pygelf import GelfUdpHandler
import config
from logsys.message_codes import MessageCodes


graylog_handler = GelfUdpHandler(host=config.logger_host, port=config.logger_port)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('data-lake-pipelines')
logger.handlers = []
logger.propagate = config.logger_allow_std_output
logger.addHandler(graylog_handler)


class LogMan(object):
    def __init__(self):
        self.codes = MessageCodes()

    @staticmethod
    def log_message(message: str):
        logger.info(message)

    def info(self, code: str, *args):
        message = self.codes.get_info_message(code, *args)
        LogMan.log_message(message=message)
        return message

    def error(self, code: str, *args):
        message = self.codes.get_error_message(code, *args)
        LogMan.log_message(message=message)
        return message
