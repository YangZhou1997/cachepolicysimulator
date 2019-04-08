import re
import numpy as np
import pandas as pd
from collections import OrderedDict
import json
from main import main

df = pd.read_csv('alibaba-trace/batch_task.csv')
df.columns = ['stage_name','task_num','job_name','stage_type','status','start_time','end_time','plan_cpu','plan_mem']
df['stage_id'] = np.zeros(len(df),dtype=int)
df=df.loc[~df['stage_name'].str.contains("ask")] # remove the indistinguishable tasks /Task
stage_num=df.groupby('job_name',as_index=False).count()
stage_num=stage_num.sort_values(by=['stage_name'], ascending=False)
jobs_with_largest_task_num=stage_num['job_name'].iloc[0:200]

count = 0
for job_name in jobs_with_largest_task_num.tolist():
    job_profile = OrderedDict()
    stages = df.loc[df['job_name']==job_name]
    stages=stages.sort_values(by=['start_time'])
    base_time = min(stages['start_time'])
    loop = False # some job has loop dependency, skip such jobs. e.g. 'j_3734942'
    try:
        for index, stage in stages.iterrows():
            stage_id = int(re.findall('[A-Z]([0-9]+)', stage['stage_name'])[0])
            #stage['stage_id'] = int(stage_id)
            stages.at[index,'stage_id'] = int(stage_id)
            parent_ids =re.findall('_([0-9]+)',stage['stage_name'])
            parent_ids = [int(parent_id) for parent_id in parent_ids]
            job_profile[stage_id] = {}
            job_profile[stage_id]["Task_Num"]=int(stage['task_num'])
            job_profile[stage_id]["Parents"] = parent_ids
            job_profile[stage_id]["Start_Time"]=stage['start_time'] - base_time
            job_profile[stage_id]["End_Time"]=stage['end_time'] - base_time
            # detect loops:
            for parent_id in parent_ids:
                if parent_id in job_profile and stage_id in job_profile[parent_id]['Parents']:
                    loop = True
                    break
    except:
        continue
    if loop:
        continue

    # the timestamp in the trace has quality problem, so we generate by ourselves
    stages=stages.sort_values(by=['stage_id'])
    stage_endtime_map = dict()
    complete_stage_id_list=list()
    while len(complete_stage_id_list) < len(stages):
        for stage_id in stages['stage_id']:
            if stage_id not in complete_stage_id_list:
                start_time = 0  # if no dependency
                try:
                    if len(job_profile[stage_id]["Parents"]) > 0:
                        start_time = max([stage_endtime_map[parent_id] for parent_id in job_profile[stage_id]["Parents"]])+0.1
                except: # some parent has not completed
                    continue
                end_time = start_time + np.random.exponential(5)
                stage_endtime_map[stage_id]  = end_time
                job_profile[stage_id]["Start_Time"]  = start_time
                job_profile[stage_id]["End_Time"] = end_time
                complete_stage_id_list.append(stage_id)

    with open('job-profile/%s.json' % job_name, 'w') as f:
        json.dump(job_profile, f)
    # One trace issue: some tasks' starts before its dependent tasks ends.
    count+=1
    if count==100:
        break
#main([1, 1000])


# flow_sizes =list()
# for line in f.readlines():
#     mapper_num = int(line.split(' ')[2])
#     sizes = re.findall(':(.*?)\s', line)
#     sizes=np.array(sizes)
#     sizes=np.asfarray(sizes, float)
#     sizes /= mapper_num
#     for size in sizes:
#         flow_sizes.append(size)
#
# f.close()
# f=open('coflow-benchmark/flow_size.txt','w')
# for size in flow_sizes:
#     f.write('%s\t'%size)
# f.close()
