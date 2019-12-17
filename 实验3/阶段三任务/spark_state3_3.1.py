from pyspark import SparkContext
from operator import add

def map_func(x):
    x = x.strip()
    s = x.split(',')
    return (s[4],int(s[7]))


if __name__ == "__main__":

    sc=SparkContext(appName='spark_state3')

    #得到初始RDD
    lines=sc.textFile("/Users/shijianjun/学习资料/金融大数据处理/作业/实验3/双十一数据/million_user_log.csv").map(lambda x:map_func(x)).cache()
    #筛选出浏览的数据
    view=lines.filter(lambda x: x[1]==0)
    #改变key值为省+类，并求和
    brand = view.map(lambda x: (x[0],1)).reduceByKey(add).sortBy(keyfunc=lambda x: x[1],ascending=False).take(10)


    for (key,value) in brand:
        print((key,value))
        print('\n')
