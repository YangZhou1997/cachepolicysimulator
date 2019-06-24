
This project aims to provide a simulator to examine the performance of cache replacement policies for data analytics systems. We replay the workloads from [the Alibaba Trace](https://github.com/alibaba/clusterdata). Currently，this simulator supports three cache replacement policies, the Least-Recently-Used (LRU) policy, the least-Reference-Count ([LRC](https://home.cse.ust.hk/~weiwa/papers/lrc-infocom17.pdf)) and [MEMTUNE](https://ieeexplore.ieee.org/document/7516034). Readers can also test their own cache policies following the guidance below (Step 3).


## Step 1. Parse the Alibaba trace and obtain the job DAGs.
Run `parse_alibaba_trace.py`.

### Notes
1. The parsed DAG will be output to the directory `job-profile` in the form of JSON files. Each JSON file represents a job. The file name is exactly the job name in the trace.

The structure of the JSON files is as follows.
{ "\[stage-id\]",

}


2. By default, this script will parse the first 200 jobs with the most complex (number of tasks) in the trace. Can be changed in line 14 of `parse_alibaba_trace.py`.

