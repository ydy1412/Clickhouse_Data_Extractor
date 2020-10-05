import signal

from CLICK_HOUSE_DATA_EXTRACTOR import Click_House_Data_Extractor

class Worker :
    def __init__(self, id, log_file):
        self.id = id
        self.log_file = log_file

    def main(self):
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        # 이 부분 수정 필요.
        self.Click_House_Extractor = Click_House_Data_Extractor()

        ## 이 부분 수정 필요.
        res = self.Click_House_Extractor.main()
        return res


    def stop(self, signum, frame):
        self.Click_House_Extractor.stop()