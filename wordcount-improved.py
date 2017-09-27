from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string
import unicodedata
 
inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+
 
def words_once(line):
	wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
	for w in wordsep.split(line.lower()):
		yield (unicodedata.normalize('NFC', w), 1)
 
def get_key(kv):
	return kv[0]

def get_val(kv):
	return kv[1]

#using a one liner lambda instead
#def filter_empty_strings(kv):
	#return kv[0]!=""
 
def output_format(kv):
	k, v = kv
	return '%s %i' % (k, v)
 
text = sc.textFile(inputs)
words = text.flatMap(words_once)

wordcount = words.reduceByKey(operator.add)

#plain output
outdata = wordcount.filter(lambda x: x[0]!="")
outdata.saveAsTextFile(output)

#outdata with sort and map
outdata = wordcount.filter(lambda x: x[0]!="").sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output+ '/by-word')

#outdata with sort by freq and map
outdata = wordcount.filter(lambda x: x[0]!="").sortBy(get_val).map(output_format)
outdata.saveAsTextFile(output + '/by-freq')
