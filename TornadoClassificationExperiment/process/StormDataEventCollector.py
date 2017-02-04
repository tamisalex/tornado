'''
Created on Jan 3, 2017

@author: alexandertam
'''

import pandas as pd 
import sqlalchemy
import urllib2
from bs4 import BeautifulSoup
import requests
from StringIO import StringIO
import gzip

connectionString = "postgresql://axt4989:ekye6emNonagon9@tamisalex.cbpjsu8olcvg.us-east-1.rds.amazonaws.com:5432/weather"

class StormDataEventCollector(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
       
       
    
    
    def getFile(self,filename):
        baseURL = "http://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
    
        response = urllib2.urlopen(baseURL + filename)
        compressedFile = StringIO()
        compressedFile.write(response.read())
        #
        # Set the file's current position to the beginning
        # of the file so that gzip.GzipFile can read
        # its contents from the top.
        #
        compressedFile.seek(0)
    
        decompressedFile = gzip.GzipFile(fileobj=compressedFile, mode='rb')
    
        return decompressedFile


    def getFileList(self):
        weatherDataUrl = "http://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
        soup = BeautifulSoup(requests.get(weatherDataUrl).text, "lxml")
        fileList = []
        for a in soup.find_all('a'):
                if "StormEvents" in a["href"]:
                    fileList.append(a["href"])
        return fileList


    def getFile_Convert_Append(self,filename,df):
        csvFile = self.getFile(filename)
        csv = pd.read_csv(csvFile)
        csv.columns = map(str.lower,csv.columns)
        df = df.append(csv)
        return df

    def collect(self):
        engine = sqlalchemy.create_engine(connectionString)
    
        
        #fileTypes = ["details","locations","fatalities"]
        years = range(2015,2016)
        years = map(str,years)
        detailsDF = pd.DataFrame()
        locationsDF = pd.DataFrame()
        fatalitiesDF = pd.DataFrame()
        
        for filename in self.getFileList():
            if any(year in filename for year in years):
                if("details" in filename):
                    detailsDF = self.getFile_Convert_Append(filename,detailsDF)
                    continue
                if("fatalities" in filename):
                    fatalitiesDF = self.getFile_Convert_Append(filename,fatalitiesDF)
                    continue
                if("locations" in filename):
                    locationsDF = self.getFile_Convert_Append(filename,locationsDF)
                    continue
                
        #TornadoesDF = detailsDF[detailsDF["event_type"]=="Tornado"]
        
        fatalitiesDF.to_sql("fatalities",con = engine, if_exists = "replace")
        
        AlabamaDF = detailsDF[detailsDF["state"] ==  "ALABAMA"].copy()
        AlabamaDF = AlabamaDF[AlabamaDF["wfo"] != "TAE"]
        AlabamaDF = AlabamaDF[["wfo","episode_id","event_id","event_type","begin_date_time","end_date_time","begin_lat","begin_lon"]]
        AlabamaDF = AlabamaDF.dropna()
        try:
            
            AlabamaDF.to_sql("alabama",con = engine, if_exists = "replace")
            locationsDF.to_sql("locations",con = engine, if_exists = "replace")
        except:
            print "write to db failed"


# In[ ]:   