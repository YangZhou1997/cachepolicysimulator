from job import Job
from task import Task
#from block_manager import BlockManager

class Cluster:

    def __init__(self, block_manager):
        self.block_manager = block_manager

    def submit_task(self, task,time):
        task_hit = False
        [hit_count, miss_count] = self.block_manager.get_blocks(task.dependent_blocks,time)
        self.block_manager.put_block(task.produced_blocks,time)
        if(miss_count==0):
            task_hit=True
        task.hit_num = hit_count
        task.miss_num=miss_count
        task.task_hit = task_hit
        return

    def reset(self):
        self.block_manager.reset()



