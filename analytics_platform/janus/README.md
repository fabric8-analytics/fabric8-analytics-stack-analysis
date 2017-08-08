# Janus Model

Model to cluster/group users based on the user's metadata.

## To run and test the model have the following setup:
* Java (with path added to bashrc)
* Spark (with path added to bashrc)
* Maven (with path added to bashrc)
* Docker and Docker-compose


## To train and score locally(trained locally but results saved on AWS)
* `cp config.py.template config.py`
* Set up the .env with correct parameters
* `cd $SPARK_HOME`
* Train the model `PYTHONPATH=<path to kattappa> ~/kattappa/analytics_platform/janus/src/training.py --packages org.apache.hadoop:hadoop-aws:<version as per spark-hadoop>` *NOTE
* Batch Score the model `PYTHONPATH=<path to kattappa> ~/kattappa/analytics_platform/janus/src/batch_score.py --verbose --packages org.apache.hadoop:hadoop-aws:<version as per spark-hadoop>` *NOTE
* NOTE: If installed from source, the Spark-Hadoop version can be obtained by
  * `cd $SPARK_HOME`
  * `cat RELEASE`
  * For me the version is `Spark 2.1.1 built for Hadoop 2.7.3`, and my spark-submit command looks like
  `PYTHONPATH=~/kattappa ~/kattappa/analytics_platform/janus/src/training.py --packages org.apache.hadoop:hadoop-aws:2.7.3`
  
  
## To train and score on Spark-EMR(trained on AWS)
* `cd kattappa`
* `docker-compose -f docker-compose-janus.yaml build`
* `docker-compose -f docker-compose-janus.yaml up`
* On another terminal `curl -XPOST http://0.0.0.0:6003/api/v1/janus/training`
* Go to the EMR console and view the job logs.
