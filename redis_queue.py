# -*- coding: utf-8 -*-
"""
Created on Sat Sep  2 07:45:53 2017

@author: my7pro
"""
import redis

r = redis.Redis()

class RQueue(object):
    """An abstract FIFO queue"""
    def __init__(self, local_id=None):
        if local_id is None:
            local_id = r.incr("queue_space")
        id_name = "q:%s" %(local_id)
        self.id_name = id_name

    def push(self, element):
        """Push an element to the tail of the queue""" 
        id_name = self.id_name
        push_element = r.rpush(id_name, element)
    
    def pop(self):
        """Pop an element from the head of the queue"""
        id_name = self.id_name
        popped_element = r.lpop(id_name)
        return popped_element
