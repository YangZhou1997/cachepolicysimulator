class RDD:
    def __init__(self, name, partitions):
        self.name = name
        self.blocks = list()
        self.partitions = partitions
        self.ref_count = 0 # initial value, will not be updated during the execution process

        #self.is_complete = True
        self.user_id = 1
        #self.peer_rdd_names=list()

    def set_blocklist(self,blocklist):
        self.blocks = blocklist


    # Aggressive: update the ref-counts of peers of all blocks.

    #
    # def ref_me_block_incomplete(self, index):
    #     if self.is_shuffle:
    #         for block in self.blocks:
    #             block.eff_ref_count_blk -= 1
    #     else:
    #         self.blocks[index].eff_ref_count_blk -= 1
    #
    # def ref_me_rdd_incomplete(self, partitions):
    #     for block in self.blocks:
    #         if self.is_shuffle:
    #             block.eff_ref_count_rdd -= partitions
    #         else:
    #             block.eff_ref_count_rdd -= 1

