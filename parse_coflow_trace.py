import re
import numpy as np

f=open('coflow-benchmark/FB2010-1Hr-150-0.txt','r')
f.readline() # remove header
flow_sizes =list()
for line in f.readlines():
    mapper_num = int(line.split(' ')[2])
    sizes = re.findall(':(.*?)\s', line)
    sizes=np.array(sizes)
    sizes=np.asfarray(sizes,float)
    sizes /= mapper_num
    for size in sizes:
        flow_sizes.append(size)

f.close()
f=open('coflow-benchmark/flow_size.txt','w')
for size in flow_sizes:
    f.write('%s\t'%size)
f.close()
