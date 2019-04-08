import sys
import os
from cluster import Cluster
from simulator import Simulator
from block_manager import BlockManager
from math import pow
from policy import *

def main(argv):  #argv: user_number,cache_size total (in MB)

    user_number = int(argv[0])
    cache_size = int(argv[1])
    block_manager = BlockManager()
    cluster = Cluster(block_manager)
    simulator = Simulator(cluster, user_number)
    cur_dir = os.getcwd()  ## get current working directory
    summary_lru =open("%s/summary_lru.txt" % cur_dir, "w")
    summary_lrc =open("%s/summary_lrc.txt" % cur_dir, "w")
    summary_lrc_con =open("%s/summary_lrc_con.txt" % cur_dir, "w")
    summary_lrc_agg =open("%s/summary_lrc_agg.txt" % cur_dir, "w")
    summary_memtune =open("%s/summary_memtune.txt" % cur_dir, "w")

    for filename in os.listdir("job-profile"):
    #for repeat in range(1,50):
        job_name = filename.split('.')[0]
        Simulator.jobs[0] = job_name
        summary = open("%s/summary.txt" % cur_dir, "a+")  # log for the cache performance of different cache policies
        summary.write("Job %s\n" % (job_name))
        summary.write("%s MB memory\n" % (cache_size))

        simulator.cluster.block_manager.set_policy(LRUPolicy(cache_size))
        [total_hit, total_miss, total_task_hit, total_task_miss, total_stage_hit, total_stage_miss] = simulator.run()
        print total_hit, total_miss, total_task_hit, total_task_miss, total_stage_hit, total_stage_miss
        summary.write("%s: %s\t%s\t%s\n" % (simulator.cluster.block_manager.policy.name, float(total_hit) / (total_hit + total_miss),
            float(total_task_hit)/(total_task_hit + total_task_miss),float(total_stage_hit)/(total_stage_hit + total_stage_miss)))
        summary_lru.write("%s\t%s\t%s\n" % (float(total_hit) / (total_hit + total_miss),
            float(total_task_hit)/(total_task_hit + total_task_miss),float(total_stage_hit)/(total_stage_hit + total_stage_miss)))

        #simulator.reset()

        simulator.cluster.block_manager.set_policy(LRCPolicy(cache_size))
        [total_hit, total_miss, total_task_hit, total_task_miss, total_stage_hit, total_stage_miss] = simulator.run()
        print total_hit, total_miss, total_task_hit, total_task_miss, total_stage_hit, total_stage_miss
        summary.write("%s: %s\t%s\t%s\n" % (simulator.cluster.block_manager.policy.name, float(total_hit) / (total_hit + total_miss),
            float(total_task_hit)/(total_task_hit + total_task_miss),float(total_stage_hit)/(total_stage_hit + total_stage_miss)))
        summary_lrc.write("%s\t%s\t%s\n" % (float(total_hit) / (total_hit + total_miss),
            float(total_task_hit)/(total_task_hit + total_task_miss),float(total_stage_hit)/(total_stage_hit + total_stage_miss)))
        #simulator.reset()

        simulator.cluster.block_manager.set_policy(LRCConservativePolicy(cache_size))
        [total_hit, total_miss, total_task_hit, total_task_miss, total_stage_hit, total_stage_miss] = simulator.run()
        print total_hit, total_miss, total_task_hit, total_task_miss, total_stage_hit, total_stage_miss
        summary.write("%s: %s\t%s\t%s\n" % (simulator.cluster.block_manager.policy.name, float(total_hit) / (total_hit + total_miss),
            float(total_task_hit)/(total_task_hit + total_task_miss),float(total_stage_hit)/(total_stage_hit + total_stage_miss)))
        summary_lrc_con.write("%s\t%s\t%s\n" % (float(total_hit) / (total_hit + total_miss),
            float(total_task_hit)/(total_task_hit + total_task_miss),float(total_stage_hit)/(total_stage_hit + total_stage_miss)))
        #simulator.reset()

        simulator.cluster.block_manager.set_policy(LRCAggressivePolicy(cache_size))
        [total_hit, total_miss, total_task_hit, total_task_miss, total_stage_hit, total_stage_miss] = simulator.run()
        print total_hit, total_miss, total_task_hit, total_task_miss, total_stage_hit, total_stage_miss
        summary.write("%s: %s\t%s\t%s\n" % (simulator.cluster.block_manager.policy.name, float(total_hit) / (total_hit + total_miss),
            float(total_task_hit)/(total_task_hit + total_task_miss),float(total_stage_hit)/(total_stage_hit + total_stage_miss)))
        summary_lrc_agg.write("%s\t%s\t%s\n" % (float(total_hit) / (total_hit + total_miss),
            float(total_task_hit)/(total_task_hit + total_task_miss),float(total_stage_hit)/(total_stage_hit + total_stage_miss)))

        #simulator.reset()

        simulator.cluster.block_manager.set_policy(MemTune(cache_size))
        [total_hit, total_miss, total_task_hit, total_task_miss, total_stage_hit, total_stage_miss] = simulator.run()
        print total_hit, total_miss, total_task_hit, total_task_miss, total_stage_hit, total_stage_miss
        summary.write("%s: %s\t%s\t%s\n" % (simulator.cluster.block_manager.policy.name, float(total_hit) / (total_hit + total_miss),
            float(total_task_hit)/(total_task_hit + total_task_miss),float(total_stage_hit)/(total_stage_hit + total_stage_miss)))
        summary_memtune.write("%s\t%s\t%s\n" % (float(total_hit) / (total_hit + total_miss),
            float(total_task_hit)/(total_task_hit + total_task_miss),float(total_stage_hit)/(total_stage_hit + total_stage_miss)))

        #simulator.reset()

if __name__ == "__main__":
    #user_number, total cache_size (in MB)
    main([1, 1000])#(sys.argv[1:])
