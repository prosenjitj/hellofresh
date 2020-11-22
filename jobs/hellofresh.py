"""     hellofresh.py
        ~~~~~~~~~~~~~~~~~~~~
        Using Apache Spark and Python read processed dataset from step 1 and:

        extract only recipes that have beef as one of the ingredients
        calculate average cooking time duration per difficulty level
        Total cooking time duration can be calculated by formula:

        total_cook_time = cookTime + prepTime
        Criteria for levels based on total cook time duration:

        easy - less than 30 mins
        medium - between 30 and 60 mins
        hard - more than 60 mins.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col , lower, udf
from pyspark.sql.types import IntegerType, StringType
import re


# UDF should be independent of class.So, this functions are kept outside of main object.
def convertPTToTime(st) :
    tm = re.match('PT(\d+)(\w+)', st)
    if not tm :
        return 0
    elif tm.group(2) == 'M':
        return int(tm.group(1))
    elif tm.group(2) == 'H':
        return int(tm.group(1)) * 60
    return 0

def markLevel (totalTime) :
    if totalTime < 30 :
        return 'easy'
    elif totalTime > 60 :
        return 'hard'
    else :
        return 'medium'

# Predefined UDF functions to filter and seperate out each row based on total time
udfConvertToPTTime = udf(lambda x : convertPTToTime(x), IntegerType())
udfMarkLevel = udf(lambda z: markLevel(z), StringType())



class HelloFreshRecipeTest :
    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("HelloFresh") \
            .getOrCreate()


    # E : Extract Data from Source System
    def extractData(self) :
        try:
            # Reading file downloded from S3 (https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json)
            return self.spark.read.format("json").option('encoding', 'UTF-8').json("data/recipes.json")
        except :
            print("Exception on Extracting Data")
        return None


    # L : Load data to External File
    def loadData(self, df):
        try :
            df.toPandas().to_csv('output/report.csv', index=False, encoding='cp932')
        except :
            print("Failed to save report")



    # T : Transform Extracted Data
    def transformData(self, df) :

        # Calculate Totoltime for row
        dfTotalTime = df.select(['cookTime' , 'prepTime']) \
            .withColumn('totalTime' , udfConvertToPTTime(col('cookTime')) + udfConvertToPTTime(col('prepTime')))

        # Mark each difficulty level
        dfTotalTime = dfTotalTime.withColumn('difficulty' , udfMarkLevel(col('totalTime')))
        print("Data Transformed Successfully !!")

        # Step 2 : calculate average cooking time duration per difficulty level
        # Create Report based on average value
        return dfTotalTime.groupBy("difficulty").agg({'totalTime':'avg'}).alias('avg_total_cooking_time')



    # Combine ETL flow to build report
    def buildRecipeReport(self):

        # Step 1 : extract only recipes that have beef as one of the ingredients
        df = self.extractData()
        if not df or len(df.take(1)) == 0 :
            print("No Data Present")
        else :
            df = df.select(['cookTime' , 'prepTime' ]) \
               .where(lower(col('ingredients')).rlike("(?i)beef"))

            self.loadData(self.transformData(df))
            self.spark.stop()

if __name__ == "__main__":
    recipeTest = HelloFreshRecipeTest()
    recipeTest.buildRecipeReport()

