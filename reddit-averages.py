from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string
import unicodedata

import json
 
inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('reddit average')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+
 
def words_once(line):
	j = json.loads(line)
	yield (j['subreddit'], (1, j['score']) )
 
def get_key(kv):
	return kv[0]

def get_val(kv):
	return kv[1]

def add_pairs(x, y):
	return (x[0] + y[0], x[1] + y[1])
	
def output_format(kv):
	subreddit, (count, score) = kv
	#return '[\"%s\", %s]' % (subreddit, score/count)
	return json.dumps((subreddit, score/count))

text = sc.textFile(inputs)
words = text.flatMap(words_once).reduceByKey(add_pairs).map(output_format)
print(words.take(10))
words.coalesce(1).saveAsTextFile(output)

#wordcount = words.reduceByKey(operator.add)

#plain output
#outdata = wordcount
#outdata.saveAsTextFile(output)

#outdata with sort and map
#outdata = wordcount.sortBy(get_key).map(output_format)
#outdata.saveAsTextFile(output+ '/by-word')
