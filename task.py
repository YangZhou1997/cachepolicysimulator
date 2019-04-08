class Task:
    def __init__(self, user_id, stage_id, index):
        self.user_id = user_id
        self.stage_id = stage_id
        self.index = index
        self.name = 'user%s_stage%s_task%s' % (user_id, stage_id, index)
        self.dependent_blocks = list()
        self.produced_blocks = 0 # only one block
        self.task_hit =False # Task hit
        self.hit_num=0 # Block hit
        self.miss_num=0
        self.start_time = 0
        self.end_time = 0


    def set_dependent_blocks(self, dependent_blocks):
        self.dependent_blocks = dependent_blocks

    def set_produced_block(self,produced_block):
        self.produced_blocks =produced_block

    def set_hit(self,task_hit):
        self.task_hit=task_hit

    def set_start_time(self,start_time):
        self.start_time = start_time

    def set_end_time(self,end_time):
        self.end_time = end_time

    def set_block_hit_miss(self,hit,miss):
        self.hit_num=hit
        self.miss_num=miss
