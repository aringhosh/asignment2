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
 
def output_format(kv):
	k, v = kv
	return '%s %i' % (k, v)
 
text = sc.textFile(inputs)
words = text.flatMap(words_once)
wordcount = words.filter(lambda n: n != "").reduceByKey(operator.add)
outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)
