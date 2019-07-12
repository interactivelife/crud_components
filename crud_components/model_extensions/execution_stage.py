from enum import Enum


class Stage(Enum):
    PRE_FLUSH = 'pre_flush'
    POST_FLUSH = 'post_flush'
    PRE_SET = 'pre_set'
    PRE_VERSIONING = 'pre_versioning'
    PRE_FLUSH_DELETE = 'pre_flush_delete'