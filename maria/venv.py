python3 -m venv myenv
source myenv/bin/activate
pip install kafka-python
pip install pyspark
pip install spark-sql-kafka-0-10
pip install jsonschema
pip install confluent-kafka
pip install pyspark confluent-kafka

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.2 spark.py
