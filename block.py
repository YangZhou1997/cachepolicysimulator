from random import randint

class Block:
    def __init__(self,rdd, rdd_name, index, user_id):
        self.rdd = rdd
        self.rdd_name=rdd_name
        self.index=index
        self.user_id = user_id
        self.name = '%s_block%s' % (rdd_name, index)
        self.size=self.draw_random_size()
        self.ref_count = 0
        #self.ref_count_plan=0 # for use of making eviction plans in sticky policies

        #self.is_complete = True # complete means its input blocks have not been evicted. Initialized as True
        #self.is_rdd_complete = True

    def add_ref_count(self): # Called while initializing the job profile
        self.ref_count+=1

    def decrease_ref_count(self): # Called when 1. accessed  or 2. peers get evicted
        self.ref_count-=1

    def reset_ref_count(self): # Called when get evicted with sticky poilcies
        self.ref_count-=0


    def draw_random_size(self):
        f=open('coflow-benchmark/flow_size.txt')
        sizes = f.read().split()
        sizes = [int(float(size)) for size in sizes]

        block_size = sizes[randint(0,len(sizes)-1)]
        return block_size



if __name__=='__main__':
    Block(1,1,1,1)

    # def set_incomplete(self): # Called when peers get evicted
    #     self.is_complete = False
    #
    #
    #
    #
    # # For use of making eviction plans in sticky policies
    # def load_ref_count_plan(self):
    #     self.ref_count_plan = self.ref_count
    #
    # def decrease_ref_count_plan(self):
    #     self.ref_count_plan -=1
    #
    # def make_ref_count_plan(self):
    #     self.ref_count = self.ref_count_plan
