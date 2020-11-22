# HelloFresh Data Engineering Solution

It can be submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    > ./startUp.sh

    or

    > $SPARK_HOME/bin/spark-submit \
        --master local[*] \
        --files configs/etl_config.json \
        jobs/hellofresh.py


    Where , $SPARK= <Replace by actual spark directory>

     Folder Structure :
     configs : project configuration
     data : source data
     output : report
     logs : generated logs
     job : application

     