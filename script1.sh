export SPARK_HOME=/Users/aringhosh/spark-2.2.0-bin-hadoop2.7/
export PYSPARK_PYTHON=python3

rm -rv out-1/
${SPARK_HOME}/bin/spark-submit wordcount.py wordcount-1/ out-1/