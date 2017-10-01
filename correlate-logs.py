from pyspark import SparkConf, SparkContext
import sys
import re
import math
 
inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('correlate logs 1')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def parseline(line):
    linere = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    match = re.search(linere, line)
    if match:
        m = re.match(linere, line)
        host = m.group(1)
        bys = float(m.group(4))
        return (host, (1, bys))
    return None

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))
	
def output_format(comp):
	(sum_x, sum_y, sum_x2, sum_y2, sum_xy, n) = comp
	r = (n*sum_xy - sum_x*sum_y)/(math.sqrt(n*sum_x2 - pow(sum_x, 2)) * math.sqrt(n*sum_y2 - pow(sum_y, 2)))
	return r, pow(r, 2)

host_bytes = sc.textFile(inputs).map(lambda line: parseline(line)).filter(lambda x: x is not None)

#words = sc.textFile(inputs).flatMap(parseline).reduceByKey(add_tuples)
print(host_bytes.collect())
#words = words.map(lambda host, bytes, n: ('all_hosts', (bytes, n, pow(bytes, 2), pow(n, 2), bytes*n, 1)))
#r_components = words.reduceByKey(lambda x,y: add_tuples(x,y)).map(lambda k,v: v)
#output_data = r_components.map(output_format)
#output_data.saveAsTextFile(output)