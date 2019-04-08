import abc
from policy_name import Policy_Name

class Policy():
    __metaclass__ = abc.ABCMeta

    def __init__(self,cache_size):
        self.rdd_list=list()
        self.cache_list = list()
        self.cache_size = cache_size
        self.remaining_cache_size = cache_size

    def set_rdd_list(self,rdd_list):
        self.rdd_list= rdd_list.copy()

    @abc.abstractmethod
    def __access__(self,block):
        raise NotImplementedError('users must define __access__ to use this base class')

    def put_block(self, block):
        if(self.cache_size < block.size):
            return False # exceed total capacity
        # cache the block on miss
        result=True
        if(self.remaining_cache_size < block.size):
            result=self.__make_room_for__(block)
        if(result): # successfully made room
            self.cache_list.append(block)
            self.remaining_cache_size-= block.size
        return result

    @abc.abstractmethod
    def __make_room_for__(self,block):
        raise NotImplementedError('users must define __make_room_for__ to use this base class')


class LRUPolicy(Policy):
    def __init__(self,cache_size):
        super(self.__class__, self).__init__(cache_size)
        self.name=Policy_Name.lru

    def __access__(self,block):
        if(block in self.cache_list):
            self.cache_list.remove(block)
            self.cache_list.append(block) # Put it back to the tail of the list (MRU position)
            return True # cache hit
        else:
            self.put_block(block) # cache the block on miss
            return False # cache miss

    def __make_room_for__(self,block):
        to_evict = list()
        to_free_space = 0
        for to_evict_block in self.cache_list: # evict from the head of list (LRU position)
            to_evict.append(to_evict_block)
            to_free_space += to_evict_block.size
            if(to_free_space >= block.size):
                break
        # do eviction
        for to_evict_block in to_evict:
            self.cache_list.remove(to_evict_block)
        self.remaining_cache_size += to_free_space
        return True

class LRCPolicy(Policy):
    def __init__(self,cache_size):
        super(self.__class__, self).__init__(cache_size)
        self.name=Policy_Name.lrc

    def __access__(self,block):
        block.decrease_ref_count()
        if(block in self.cache_list):
            return True # cache hit
        else:
            self.put_block(block) # try to cache the block on miss
            return False # cache miss

    def __make_room_for__(self,block):
        to_evict = list()
        to_free_space = 0
        self.cache_list.sort(key=lambda x: x.ref_count)# checked
        for to_evict_block in self.cache_list: # evict from the block with the least ref count
            if(to_evict_block.ref_count >= block.ref_count):
                return False # do not evict data of larger/equal ref count
            to_evict.append(to_evict_block)
            to_free_space += to_evict_block.size
            if(to_free_space >= block.size):
                break # enough cleared space
        # do eviction
        for to_evict_block in to_evict:
            self.cache_list.remove(to_evict_block)
        self.remaining_cache_size += to_free_space
        return True


class LRCConservativePolicy(Policy):
    def __init__(self,cache_size):
        super(self.__class__, self).__init__(cache_size)
        self.name=Policy_Name.lrc_conservative
        self.peer_groups=list()

    def add_peer_group(self,group):
        self.peer_groups.append(group)

    def __access__(self,block):
        block.decrease_ref_count()
        if(block in self.cache_list):
            return True # cache hit
        else:
            self.put_block(block) # try to cache the block on miss
            return False # cache miss

    def put_block(self, block):
        if(self.cache_size < block.size):
            return False # exceed total capacity
        # cache the block on miss
        result=True
        if(self.remaining_cache_size < block.size):
            result=self.__make_room_for__(block)
        if(result): # successfully made room
            self.cache_list.append(block)
            self.remaining_cache_size-= block.size
            if self.remaining_cache_size<0:
                db=1
        else: # persist to disk, update ref count of this block and its peers
            block.reset_ref_count()
            self.check_peer_group(block)


    def __make_room_for__(self,block):
        to_evict = list()
        to_free_space = 0

        # check whether there is enough space to clear by evicting blocks with smaller ref counts
        # For blocks in complete groups with this block, their ref counts will be decreased identically.
        self.cache_list.sort(key=lambda x: x.ref_count)
        #enough_space = False
        for to_evict_block in self.cache_list:# evict from the block with the least ref count
            if(to_evict_block.ref_count >= block.ref_count):
                return False # do not evict data of larger/equal ref count
            to_free_space += to_evict_block.size
            if(to_free_space >= block.size):
                #enough_space = True
                break # enough cleared space
        # if not enough_space:
        #     return False

        enough_space = False
        to_free_space=0
        while(True): # iteratively update the effective ref counts until enough space is cleared or no more blocks can be evicted
            self.cache_list.sort(key=lambda x: x.ref_count)
            for to_evict_block in self.cache_list: # evict from the block with the least ref count
                if(to_evict_block not in to_evict):
                    to_evict.append(to_evict_block)
                    to_free_space += to_evict_block.size
                    to_evict_block.reset_ref_count() # set eff ref count to zero once evicted

                if(self.check_peer_group(to_evict_block)):
                    break
                if(to_free_space >= block.size):
                    enough_space = True
                    break # enough space cleared
            if enough_space: # enough space
                break
            else: # some peer-groups have been cleared, resort.
                continue
        # do eviction
        for to_evict_block in to_evict:
            self.cache_list.remove(to_evict_block)
        self.remaining_cache_size += to_free_space#to_evict_block.size
        return True

    def check_peer_group(self,block):
        to_clear_groups=list()
        for peer_group in self.peer_groups:
            if block in peer_group:
                # update the ref count of peers
                for peer in peer_group:
                    if(peer != block):
                        peer.decrease_ref_count()
                to_clear_groups.append(peer_group)

        if(len(to_clear_groups)==0):
            return False # this block is not in any complete peer-group
        else:
            for to_clear_group in to_clear_groups:
                self.peer_groups.remove(to_clear_group)
            return True

class LRCAggressivePolicy(Policy):
    # LRCAggressivePolicy and LRCConservativePolicy have the same implementation logic.
    # The only difference is only the identification of peer-groups, which has been seperately defined while the simulator initializes the job profile
    def __init__(self,cache_size):
        super(self.__class__, self).__init__(cache_size)
        self.name=Policy_Name.lrc_aggressive
        self.peer_groups=list()

    def add_peer_group(self,group):
        self.peer_groups.append(group)

    def __access__(self,block):
        block.decrease_ref_count()
        if(block in self.cache_list):
            return True # cache hit
        else:
            self.put_block(block) # try to cache the block on miss
            return False # cache miss

    def put_block(self, block):
        if(self.cache_size < block.size):
            return False # exceed total capacity
        # cache the block on miss
        result=True
        if(self.remaining_cache_size < block.size):
            result=self.__make_room_for__(block)
        if(result): # successfully made room
            self.cache_list.append(block)
            self.remaining_cache_size-= block.size
            if self.remaining_cache_size<0:
                db=1
        else: # persist to disk, update ref count of this block and its peers
            block.reset_ref_count()
            self.check_peer_group(block)


    def __make_room_for__(self,block):
        to_evict = list()
        to_free_space = 0

        # check whether there is enough space to clear by evicting blocks with smaller ref counts
        # For blocks in complete groups with this block, their ref counts will be decreased identically.
        self.cache_list.sort(key=lambda x: x.ref_count)
        #enough_space = False
        for to_evict_block in self.cache_list:# evict from the block with the least ref count
            if(to_evict_block.ref_count >= block.ref_count):
                return False # do not evict data of larger/equal ref count
            to_free_space += to_evict_block.size
            if(to_free_space >= block.size):
                #enough_space = True
                break # enough cleared space
        # if not enough_space:
        #     return False

        enough_space = False
        to_free_space=0
        while(True): # iteratively update the effective ref counts until enough space is cleared or no more blocks can be evicted
            self.cache_list.sort(key=lambda x: x.ref_count)
            for to_evict_block in self.cache_list: # evict from the block with the least ref count
                if(to_evict_block not in to_evict):
                    to_evict.append(to_evict_block)
                    to_free_space += to_evict_block.size
                    to_evict_block.reset_ref_count() # set eff ref count to zero once evicted

                if(self.check_peer_group(to_evict_block)):
                    break
                if(to_free_space >= block.size):
                    enough_space = True
                    break # enough space cleared
            if enough_space: # enough space
                break
            else: # some peer-groups have been cleared, resort.
                continue
        # do eviction
        for to_evict_block in to_evict:
            self.cache_list.remove(to_evict_block)
        self.remaining_cache_size += to_free_space#to_evict_block.size
        return True

    def check_peer_group(self,block):
        to_clear_groups=list()
        for peer_group in self.peer_groups:
            if block in peer_group:
                # update the ref count of peers
                for peer in peer_group:
                    if(peer != block):
                        peer.decrease_ref_count()
                to_clear_groups.append(peer_group)

        if(len(to_clear_groups)==0):
            return False # this block is not in any complete peer-group
        else:
            for to_clear_group in to_clear_groups:
                self.peer_groups.remove(to_clear_group)
            return True

class MemTune(Policy):
    def __init__(self,cache_size):
        super(self.__class__, self).__init__(cache_size)
        self.name=Policy_Name.memTune
        self.finished_list=list()

    def __access__(self,block):
        if(block in self.cache_list):
            if block not in self.finished_list:
                self.finished_list.append(block)
            return True # cache hit
        else:
            return False # cache miss

    def __make_room_for__(self,block):
        to_evict = list()
        to_free_space = 0
        for to_evict_block in self.finished_list: # evict blocks in the finished_list
            if(to_evict_block in self.cache_list):
                to_evict.append(to_evict_block)
                to_free_space += to_evict_block.size
                if(to_free_space >= block.size):
                    break

        # Continue to evict if clearing the finished_list is not enough
        for to_evict_block in self.cache_list: # evict from the head of list (LRU position)
            if to_evict_block not in to_evict:
                to_evict.append(to_evict_block)
                to_free_space += to_evict_block.size
                if(to_free_space >= block.size):
                    break

        # do eviction
        for to_evict_block in to_evict:
            self.cache_list.remove(to_evict_block)
            if to_evict_block in self.finished_list:
                self.finished_list.remove(to_evict_block)
        self.remaining_cache_size += to_free_space
        return True
