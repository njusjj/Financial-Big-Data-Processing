from pyspark import SparkContext
from operator import add

def map_func(x):
    x = x.strip()
    s = x.split(',')
    return (s[10],[s[1],s[2],int(s[7])])

def sort_fun(y):
    all_cat=sorted(y,key=lambda x: x[1],reverse=True)
    ten_cat=all_cat[:10]
    return ten_cat


if __name__ == "__main__":

    sc=SparkContext(appName='spark_state3')

    #得到初始RDD
    lines=sc.textFile("/Users/shijianjun/学习资料/金融大数据处理/作业/实验3/双十一数据/million_user_log.csv").map(lambda x:map_func(x)).cache()
    #筛选出购买的物品
    purchase=lines.filter(lambda x: x[1][2]==2)
    #改变key值为省+类，并求和
    catogories = purchase.map(lambda x: ((x[0],x[1][1]),1)).reduceByKey(add)
    #分组，组内排序
    catogories2 = catogories.map(lambda x: (x[0][0],(x[0][1],x[1]))).groupByKey().mapValues(sort_fun).collect()

    for (key,value) in catogories2:
        print((key,value))
        print('\n')
