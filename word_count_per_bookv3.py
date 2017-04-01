#!/usr/bin/env python
import re
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
import pydoop.hdfs as hdfs
import pydoop.hdfs.path as hpath

class Mapper(api.Mapper):
    def map(self, context):
		WORD_RE = re.compile(r"[\w']+")
		file_path_temp = context.input_split.filename
		directory_path_temp = file_path_temp.rsplit("-",1) 
		file_name = "/"+context.value
		file_path = hpath.join(directory_path_temp[0],file_name)
		text = hdfs.load(file_path)
		dict = {}
		
		for w in WORD_RE.findall(text):
			w = w.lower()
			dict.setdefault(w,[]).append(int(1))
		
		context.emit(context.value,dict)
	 	
class Reducer(api.Reducer):
    def reduce(self, context):
		dict = {}
		context.emit("Document Name:",context.key)
		for val in context.values:
			for k in val.keys():
				context.emit(k, sum(val.get(k)))

def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer))
