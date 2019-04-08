import os
import json
import random
from collections import OrderedDict
from logger import Log
from rdd import RDD
from block import Block
from job import Job
from stage import Stage
from task import Task
from event import (EventTaskComplete, EventTaskSubmit)
from policy import *
import numpy as np

try:
    import Queue as Q  # ver. < 3.0
except ImportError:
    import queue as Q

class Simulator:
    jobs = OrderedDict() #Map from user id to job id
    jobs[0] = 'j_3093365'
    '''
    Init
    '''
    def __init__(self, cluster, user_number):
        self.cluster = cluster
        # self.app_name = json_dir.split('/')[-1]
        self.log = Log()
        self.cluster = cluster
        self.rdd_list = list()
        self.block_list = list()
        self.job_list = list()
        self.event_queue = Q.PriorityQueue()
        self.user_number = user_number


    '''
    Reset the ref counts
    '''
    def reset(self):
        self.log.flush()
        self.cluster.reset()
        self.rdd_list = list()
        self.block_list = list()
        self.job_list = list()
        self.event_queue = Q.PriorityQueue()

        # generate the stage list for each user. update the block ref counts
        for user_index in range(0, self.user_number):
            job_id = Simulator.jobs[user_index]
            job_profile_path = 'job-profile/%s.json' % (job_id)
            if not os.path.exists(job_profile_path):
                print('Error: can not find %s' % job_profile_path)
                exit(-1)
            else:
                job_profile = json.load(open(job_profile_path, 'r'), object_pairs_hook=OrderedDict)
                self.generate_blocks(user_index,job_profile)
                self.generate_tasks(user_index,job_profile)

        self.cluster.block_manager.rdd_list = self.rdd_list

    '''
    Search_rdd_by_name
    '''
    def search_rdd_by_name(self,name):
        for rdd in self.rdd_list:
            if rdd.name == name:
                return rdd
        return False

    '''
    Naming rule: 
        rdd: user$i_rdd$j: the rdd generated in the jth stage of user i's job 
        block: user$i_rdd$j_block$k, kth block
    '''
    def generate_blocks(self, user_id, job_profile):

        # generate all rdds and blocks
        for stage_id in job_profile.keys():
            task_num = job_profile[stage_id]['Task_Num']
            rdd_name = 'user%s_rdd%s' %(user_id,stage_id)
            this_rdd =RDD(rdd_name, task_num) # task_num in this stage = partition_num of the generated rdd
            block_list=list()
            for index in range(0, task_num):
                this_block = Block(this_rdd, this_rdd.name, index, user_id) # block size will be choson randamly according to the co-flow trace
                block_list.append(this_block)
            this_rdd.set_blocklist(block_list)
            self.rdd_list.append(this_rdd)

    '''
    Generate all stages and tasks; ref-counts of blocks and peer blocks
    '''
    def generate_tasks(self,user_id,job_profile):
        this_job = Job(user_id)
        stage_list = list()
        sorted_stage_id = np.sort(job_profile.keys()) # todo check
        for stage_id in sorted_stage_id:
            this_stage= Stage(user_id,stage_id)
            task_num = job_profile[stage_id]['Task_Num']
            this_rdd = self.search_rdd_by_name('user%s_rdd%s'%(user_id,stage_id))
            parent_rdd_ids = job_profile[stage_id]['Parents']
            start_time = float(job_profile[stage_id]['Start_Time'])
            end_time = float(job_profile[stage_id]['End_Time'])


            task_list = list()
            peer_group = list() # for sticky policies
            for task_id in range(0,task_num):
                this_task = Task(user_id,stage_id,task_id)

                # set start and end time
                this_task.set_start_time(start_time)
                this_task.set_end_time(end_time)

                # set dependent blocks and update their ref counts.
                dependent_blocks = list()
                for parent_rdd_id in parent_rdd_ids:
                    parent_rdd = self.search_rdd_by_name('user%s_rdd%s'%(user_id,parent_rdd_id))
                    dependent_block_index= task_id % parent_rdd.partitions # Map the dependent block
                    dependent_block = parent_rdd.blocks[dependent_block_index]
                    dependent_block.add_ref_count()
                    dependent_blocks.append(dependent_block)
                this_task.set_dependent_blocks(dependent_blocks)

                # set peer-groups for LRC conservative policy
                for dependent_block in dependent_blocks:
                    peer_group.append(dependent_block)
                if(isinstance(self.cluster.block_manager.policy, LRCConservativePolicy)):
                    self.cluster.block_manager.policy.add_peer_group(peer_group)
                    peer_group=list()

                # set produced_block
                produced_block = this_rdd.blocks[task_id]
                this_task.set_produced_block(produced_block)
                task_list.append(this_task)
            if(isinstance(self.cluster.block_manager.policy, LRCAggressivePolicy)):
                self.cluster.block_manager.policy.add_peer_group(peer_group)
            this_stage.set_tasks(task_list)
            stage_list.append(this_stage)

        this_job.set_stages(stage_list)
        self.job_list.append(this_job)

    '''
    Run
    '''
    def run(self):
        self.reset() # update the ref-counts

        #self.cluster.block_manager.rdd_list = self.rdd_list  # Tell the block_manager how many partitions each rdd has
        total_hit = 0
        total_miss = 0
        task_hit = 0
        task_miss = 0
        stage_hit=0
        stage_miss=0

        for job in self.job_list:
            for stage in job.stages:
                for task in stage.tasks:
                    self.event_queue.put(EventTaskSubmit(task.start_time,task))
                    self.event_queue.put(EventTaskComplete(task.end_time,task))


        while not self.event_queue.empty():
            event = self.event_queue.get()
            if isinstance(event, EventTaskSubmit):
                self.log.add('Task %s starts' % event.task.name, event.time)
                # if int(event.task.stage_id)==133:
                #     a=1
                self.cluster.submit_task(event.task, event.time)
            elif isinstance(event, EventTaskComplete):
                self.log.add('Task %s ends' %event.task.name, event.time)
                total_hit += event.task.hit_num
                total_miss += event.task.miss_num
                if(event.task.task_hit):
                    task_hit += 1
                else:
                    task_miss +=1

        # get stage hit
        for job in self.job_list:
            for stage in job.stages:
                this_stage_hit=True
                for task in stage.tasks:
                    if(task.task_hit == False):
                        this_stage_hit=False
                        break
                if(this_stage_hit):
                    stage_hit +=1
                else:
                    stage_miss +=1


        return [total_hit, total_miss, task_hit, task_miss, stage_hit, stage_miss]


