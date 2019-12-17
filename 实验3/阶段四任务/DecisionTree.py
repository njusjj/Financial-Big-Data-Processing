
from __future__ import print_function
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from sklearn.metrics import confusion_matrix
import numpy as np
import pandas as pd
from random import randint
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

def get_prediction_lists(prediction_df):
    '''
    return two lists 
    INPUTS:
    @prediction_df: Spark DF of classifier predictions 
    OUTPUTS:
    @l1: list of true labels
    @l2: list of predicted labels
    '''
    
    l1 = list(map(int, get_col_as_list(prediction_df, "label")))
    l2 = list(map(int, get_col_as_list(prediction_df, "prediction")))
    
    return l1, l2

def get_col_as_list(df, col_name):
    '''
    convert a column in Spark DF to a Python list    
    INPUTS:
    @df: Spark DF
    @col_name: name of the column 
    '''
    
    return df.select(col_name).rdd.flatMap(lambda x: x).collect()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("LogisticRegressionWithElasticNet").getOrCreate()

    # Load training data
    training = spark.read.format("libsvm").load("/Users/shijianjun/学习资料/金融大数据处理/作业/实验3/双十一数据/train_after_v2.libsvm")
    
    #划分训练集和测试集,7:3
    trainData,testData = training.randomSplit([0.7,0.3])

    dt = DecisionTreeClassifier(featuresCol='features', labelCol='label')
    evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='label')

    # init parameter grid for log rgression params
    paramGrid = ParamGridBuilder().addGrid(dt.maxDepth, [4,3]).build()

    # create cross validation object
    crossval = CrossValidator(estimator=dt,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=5) 

    # Fit the model
    #lrModel = lr.fit(trainData)
    #prediction_lr = lrModel.transform(testData)

    cvModel = crossval.fit(trainData)
    prediction_lr = cvModel.transform(testData)

    # get labels, predictions
    labels, preds= get_prediction_lists(prediction_lr)
    print('------------------------------------------------------')
    # get confusion matrix
    tn, fp, fn, tp = confusion_matrix(labels, preds).ravel()
    
    print('tn:',tn,'fp:',fp,'fn:',fn,'tp:',tp)
    # print accuracy precision, recall, and F1 score
    accuracy=float(tp+tn)/float(tp+tn+fp+fn)
    #precision=float(tp)/float(tp+fp)
    #recall=float(tp)/float(tp+fn)
    #F1score=float(2*precision*recall)/float(precision+recall)
    print ("accuracy= ", accuracy)
    #print ("precision= ", precision)
    #print ("recall= ", recall)
    #print ("F1 score= ",F1score, '\n')

    

    parameter = cvModel.getEstimatorParamMaps()
    evaluation = cvModel.avgMetrics

    param_result = []

    for params, eva in zip(parameter, evaluation):
        param_map = {}
        for key, param_val in zip(params.keys(), params.values()):
            param_map[key.name]=param_val
        param_result.append((param_map, eva))

    sorted(param_result, key=lambda x:x[1], reverse=True)
    print(param_result)
    spark.stop()

    
