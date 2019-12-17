from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark=SparkSession.builder.appName('sql').getOrCreate()

    #ames=["user_id","item_id","cat_id","merchant_id","brand_id","month","day","action","age_range","gender","province"]
    million_user_log=spark.read.csv("/Users/shijianjun/学习资料/金融大数据处理/作业/实验3/双十一数据/million_user_log_2.csv",header=True)
    
    #million_user_log=million_user_log.columns(["user_id","item_id","cat_id","merchant_id","brand_id","month","day","action","age_range","gender","province"])

    million_user_log.createOrReplaceTempView("million_user_log")
    data = spark.sql("SELECT brand_id,count(brand_id) number FROM million_user_log WHERE action=0 GROUP BY brand_id ORDER BY number DESC LIMIT 10").show()
