from logsys.logger import LogMan


# Singleton
class LogManager(object):
    class __LogManager:
        def __init__(self):
            self.client = LogMan()

    instance = None

    def __new__(cls):
        if not LogManager.instance:
            LogManager.instance = LogManager.__LogManager()
        return LogManager.instance
