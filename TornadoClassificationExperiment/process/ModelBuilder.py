'''
Created on Jan 11, 2017

@author: alexandertam
'''
import sqlalchemy
import pandas as pd
#import matplotlib.pyplot as plt
#import matplotlib
import numpy as np

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_curve, auc
from sklearn.cross_validation import train_test_split, cross_val_score, cross_val_predict
from sklearn.metrics import confusion_matrix, accuracy_score, precision_score, recall_score

from sklearn.ensemble import GradientBoostingRegressor,AdaBoostRegressor, RandomForestClassifier, RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor, DecisionTreeClassifier
from sklearn.grid_search import GridSearchCV

connectionString = "postgresql://axt4989:ekye6emNonagon9@tamisalex.cbpjsu8olcvg.us-east-1.rds.amazonaws.com:5432/weather"


class ModelBuilder(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        
    def Classifier(self, probability,threshold):
        if(probability > threshold):
            return 1
        else:
            return 0
    
    def ClassifyProbabilities(self, probabilities,threshold):
        classifieds = []
        for probability in probabilities:
            classifieds.append(self.Classifier(probability, threshold))
        return classifieds

    def CriticalSuccessIndex(self, hits, misses, falseAlarms):
        return hits/float(hits + misses + falseAlarms)

    def ActualToPredictedConfusionMatrix(self, y, predictions):
        actuals = pd.Series(y,name="Actual")
        predicted = pd.Series(predictions,name = "Predictions")
        return pd.crosstab(actuals,predicted)
    
    def PredictProbaLogisticRegression(self,X_test,y_test,model):
        #print "Model score: ", model.score(X_test,y_test)
        return model.predict_proba(X_test)
        
    def buildModel(self):
        engine = sqlalchemy.create_engine(connectionString)
        data = pd.read_sql('SELECT * FROM data',con = engine)
        data = data.dropna()
        
        try:
            del data["index"]
        except:
            pass
        y = data.loc[:,"IsTornado"]
        X = data.iloc[:,9:]
        
        logiReg = LogisticRegression()
        model = logiReg.fit(X,y)
        predictions = model.predict(X)
        
        print "Logistic Regression"
        
        print "Model score: ", model.score(X,y)
        
        print "Accuracy score: ", accuracy_score(y,predictions)
        print "Precision score: ", precision_score(y,predictions)
        print "Recall score: ", recall_score(y,predictions)
        
        print pd.crosstab(y,predictions)
        
        proba_predictions = self.PredictProbaLogisticRegression(X,y,model)
        
        newClassifieds = self.ClassifyProbabilities(pd.DataFrame(proba_predictions)[1],.1)
        
        self.ActualToPredictedConfusionMatrix(y,newClassifieds)
        print "Model score: ", model.score(X,newClassifieds)
        print "Accuracy score: ", accuracy_score(y,newClassifieds)
        print "Precision score: ", precision_score(y,newClassifieds)
        print "Recall score: ", recall_score(y,newClassifieds)
        
        cm = self.ActualToPredictedConfusionMatrix(y,newClassifieds)
        hits = cm[1][1]
        falseAlarms = cm[1][0]
        misses = cm[0][1]
        
        print "Critical Success Index: ", self.CriticalSuccessIndex(hits,misses,falseAlarms)
        cm
        
        dt = DecisionTreeClassifier(random_state=7)
        dt.fit(X,y)
        predictions = model.predict(X)
        print "Model score: ", model.score(X,y)
        
        print "Accuracy score: ", accuracy_score(y,predictions)
        print "Precision score: ", precision_score(y,predictions)
        print "Recall score: ", recall_score(y,predictions)
        