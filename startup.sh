SPARK='/Users/prosenjitdas/Documents/Spark/LatestSpark/spark-2.4.0-bin-hadoop2.7'
nohup $SPARK/bin/spark-submit \
        --master local[*] \
        --files configs/etl_config.json \
        jobs/hellofresh.py > logs/recipecalc.log 2>&1
