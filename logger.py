##log the infomation during the simulation
## the log file will be stored in  /log/data/app_name_time.txt


import datetime
import os

class Log:
    def __init__(self):
        now=datetime.datetime.now()  # [year, month, day, hour, min, second, microsecond]
        dir=os.getcwd() ## get current working directory
        [date,time] = str(now).split(" ")
        time = time.split('.')[0]
        log_dir= '%s/log/%s'%(dir, date)
        # log_dir='%s/log/%s-%s-%s'%(dir,int(datetime.datetime.year),int(datetime.datetime.month),int(datetime.datetime.day)) #log_dir= home/log/yy-mm-dd
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir)
        log_name = 'log_%s'%(time)

        self.log_file = open("%s/%s" %(log_dir, log_name), "w")

    def add(self, msg, timestamp):
        self.log_file.write('%s:\t%s\n\n'%(timestamp, msg))

    def flush(self):
        self.log_file.flush()
