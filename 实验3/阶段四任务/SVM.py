from pyspark import SparkContext
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint


if __name__ == "__main__":
    sc = SparkContext(appName="svm")

    
    # Load and parse the data
    def parsePoint(line):
        values = [float(x) for x in line.split(',')]
        return LabeledPoint(values[4], values[0:3])

    data = sc.textFile("/Users/shijianjun/学习资料/金融大数据处理/作业/实验3/双十一数据/train_after.csv")
    parsedData = data.map(parsePoint)
    
    # test = sc.textFile("/Users/shijianjun/学习资料/金融大数据处理/作业/实验3/双十一数据/test_after.csv")
    # testData = test.map(parsePoint)
    
    # split train and test
    trainData,testData=parsedData.randomSplit([0.7,0.3])
    # Build the model
    model = SVMWithSGD.train(trainData, iterations=100)
    '''
    # Evaluating the model on training data
    labelsAndPreds = testData.map(lambda p: (p.label, model.predict(p.features)))
    Recall = labelsAndPreds.filter(lambda lp: lp[0] == lp[1]).count() / float(testData.count())
    print("Recall = " + str(Recall))
    '''
    # ---------------------------------------------------------------
    labelsAndPreds = testData.map(lambda p: (p.label, model.predict(p.features)))
    Accuracy = labelsAndPreds.filter(lambda lp: lp[0] == lp[1]).count() / float(labelsAndPreds.count())
    Recall = labelsAndPreds.filter(lambda lp: lp[0] == lp[1]).filter(lambda lp: lp[0] == 1).count() / float(labelsAndPreds.filter(lambda lp: lp[0]==1).count())
    print("Accuracy = " + str(Accuracy))
    print("Recall = " + str(Recall))
