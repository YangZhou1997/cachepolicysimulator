
This project aims to provide a simulator to examine the performance of cache replacement policies for data analytics systems. We replay the workloads from [the Alibaba Trace](https://github.com/alibaba/clusterdata). Currently，this simulator supports three cache replacement policies, the Least-Recently-Used (LRU) policy, the least-Reference-Count ([LRC](https://home.cse.ust.hk/~weiwa/papers/lrc-infocom17.pdf)) and [MEMTUNE](https://ieeexplore.ieee.org/document/7516034). Readers can also test their own cache policies following the guidance below (Step 4).


## Step 1. Parse the Alibaba trace and obtain the job DAGs.
Run `parse_alibaba_trace.py`.

### Notes
1. The parsed DAG will be output to the directory `job-profile` in the form of JSON files. Each JSON file represents a job. The file name is exactly the job name in the trace.

The structure of the JSON files is as follows.

{ "\[stage-id\]":{
  {"Task_Num": \[task number\], "Parents": \[parent stages id\], "Start_Time": \[stage start time in milliseconds from the begining of the trace\], "End_Time": \[stage end time in milliseconds from the begining of the trace\]}
  }
  ...
}


2. By default, this script will parse the first 200 jobs with the most complex (number of tasks) in the trace. Can be changed in line 14 of `parse_alibaba_trace.py`.


## Step 2. Generate the input/output data size distribution.
As the Alibaba trace does not contain the information of sizes of input/output data, we refer to the [co-flow trace](https://github.com/coflow/coflow-benchmark) to get the distribution of output data block sizes in typical Map-Reduce-like jobs.

We do this by running `parse_coflow_trace.py`.

### Notes
1. Output in the file `coflow-benchmark/flow_size.txt` 

2. When running the simulation, the output data size will be drawn in randomly from the distribution we profiled from the co-flow trace. 


## Step 3. Run the simulation.
The current implementation of the simulator only supports running the jobs in sequence. 

To start the simulation, run `main.py` with one argument specifying the total cache capacity in MB. The simulator will run all the jobs profiled in the `job-profile` folder one after another.

### Notes
1. The cache performance in terms of cache hit ratio of each cache policy in the simulation will be recorded in `summary.txt`. The detailed logs will be output to the directory `log/[date]/`


## Step 4. Test your own cache replacement policy.
The cache replacement policy of this simulator is a pluggable module. All cache policies are contained in the `policy.py` file. All of them are developed from a base class `Policy`.

1. Implementation
Your cache policy should at least override two functions, namely `access` and `make_room_for`, to specify the logic of your cache policy. To avoid hard-coding your cache policy name, you can put it in the `policy_name.py` file. Please refer to the implementation of the existing three policies if you have any problem.

2. Benchmark 
To test your cache policy against the Alibaba trace and compare with other policies, please add the following code in `main.py`

`simulator.cluster.block_manager.set_policy([YOUR-POLICY-NAME](cache_size))`.

After that, run the simulator and record the results. You are all set.





