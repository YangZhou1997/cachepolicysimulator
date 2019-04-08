from enum import Enum

class Policy_Name(Enum):
    lru='LRU'
    lrc='LRC'
    lrc_conservative='LRC-Conservative'
    lrc_aggressive='LRC-Aggressive'
    memTune='MemTune'
