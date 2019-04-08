#import numpy as np
class Stage:
    def __init__(self, user_id, id):
        self.id = id
        self.user_id = user_id
        self.tasks = list()
        self.is_hit = False # stage hit
       # self.runtime = np.random.exponential(5) # the runtime from the alibaba trace is of poor quality
        #self.start_time =0
        #self.end_time =0

    def set_tasks(self,tasks):
        self.tasks = tasks

    def set_hit(self,is_hit):
        self.is_hit=is_hit
    #
    # def set_start_time(self,start_time):
    #     self.start_time = start_time
    #     self.end_time = start_time + self.runtime
