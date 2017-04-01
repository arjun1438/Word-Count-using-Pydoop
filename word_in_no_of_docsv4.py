#!/usr/bin/env python
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from collections import defaultdict
import pydoop.hdfs as hdfs

class Mapper(api.Mapper):
    def map(self, context):
		text = hdfs.load(context.value)
		f = text.split("Document Name:")
		f.pop(0)
		for i in f:
			k = i.split("\n")
			doc = k.pop(0).strip()
			for m in k:
				context.emit(m.split("\t")[0],doc)

class Reducer(api.Reducer):
    def reduce(self, context):
		count = 0	
		for val in context.values:
			count += 1
		context.emit(context.key, count)

def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer))
