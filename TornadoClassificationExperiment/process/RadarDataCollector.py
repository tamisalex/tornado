'''
Created on Jan 5, 2017

@author: alexandertam
'''

import pandas as pd 
import sqlalchemy
import datetime
import pytz
import os

import boto
import botocore
import boto3

connectionString = "postgresql://axt4989:ekye6emNonagon9@tamisalex.cbpjsu8olcvg.us-east-1.rds.amazonaws.com:5432/weather"


class RadarDataCollector(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
    
    
    # In[5]:
    
    
    
    def getTimeParts(self,utc):
        year = str(utc.year)
        month = str(utc.month)
        if len(month) == 1:
            month = "0" + month
        day = str(utc.day)
        if len(day) == 1:
            day = "0" + day
        hour = str(utc.hour)
        if len(hour) == 1:
            hour = "0" + hour
        minutes = str(utc.minute)
        if len(minutes) == 1:
            minutes = "0" + minutes
        seconds = str(utc.second)
        if len(seconds) == 1:
            seconds = "0" + seconds
        return (year,month,day,hour,minutes,seconds)
    
    
    # In[6]:
    
    def buildPath(self,station,utc):
        year, month, day, _, _, _ = self.getTimeParts(utc)
        path = year+"/"+month+"/"+day+"/K"+station+"/"
        return path
    
    
    # In[7]:
    
    def getTimePortion(self,filename):
        time = filename.split("_")[1]
        d = datetime.datetime.strptime(time, '%H%M%S')
        return d.strftime('%H:%M:%S')
        
    
    
    # In[8]:
    
    def getTimeDifferences(self,tornadoTime,ListOfTimes):
        ttime = datetime.datetime.strptime(tornadoTime, '%H:%M:%S')
        timeDifferences = []
#         minimum 
#         minimumTime
        for time in ListOfTimes:
            ftime = datetime.datetime.strptime(time, '%H:%M:%S')
            minTime = ttime - ftime
            timeDifferences.append((ftime,minTime))
        return timeDifferences       
    
    
    # In[9]:
    
    def getSelectedTimes(self,station,utc):
        # read a volume scan file on S3. Get Files for Date and Station
        s3conn = boto.connect_s3()
        bucket = s3conn.get_bucket('noaa-nexrad-level2')
    
        selectedFilenames = [key.name for key in bucket.list(self.buildPath(station,utc)) if ".gz" in key.name]     
        selectedTimes = map(self.getTimePortion,selectedFilenames)
        return selectedTimes
    
    
    # In[10]:
    
    def closestTime(self,tornadoTime,radarTimeList):
        timeTuples = []
        b_d = datetime.datetime.strptime(tornadoTime, "%H:%M:%S")
        for radarTime in radarTimeList:
            d =  datetime.datetime.strptime(radarTime, "%H:%M:%S")
            delta = b_d - d if b_d > d else datetime.timedelta.max
            timeTuples.append((delta,d))
        _, actual = min(timeTuples, key = lambda t: t[0])
        if(actual != None):
            #print actual.strftime("%H%M%S")
            return (actual.strftime("%H%M%S"),actual.strftime("%H:%M:%S"))
        #else:
            #return "hey"
    
    
    # In[11]:
    
    def buildFileID(self,station,utc,closestTimeStamp):
        year, month, day, _, _, _ = self.getTimeParts(utc)
        fileID = "K" + station + year + month +  day + "_" + closestTimeStamp
        if(int(year) >= 2012):
            fileID += "_V06.gz"
        else:
            fileID += "_V03.gz"
        return fileID
    
    
    # In[12]:
    
    def buildKey(self,station,utc,closestTimeStamp):
        path = self.buildPath(station,utc)
        fileID = self.buildFileID(station,utc,closestTimeStamp)
        key = path + fileID
        return key
        
    
    
    # In[13]:
    
    def UpdateStationCode(self,station):
        if(station == "HUN"):
            return "HTX"
        return station
    
    
    # In[14]:
    
    def GetFileFromNexradAWS(self,key,filename):
        #fileID = self.buildFileID(station,utc,closestTimeStamp)
        print "Downloading: ", filename
        try:
            s3conn = boto.connect_s3()
            bucket = s3conn.get_bucket('noaa-nexrad-level2')
            s3key = bucket.get_key(key)
            s3key.get_contents_to_filename("../assets/"+filename)
        except:
            "An error occured trying to download the file ", key
    
    
    def MoveFileFromNoaaToTornado(self,key,filename):
        s3 = boto3.resource('s3')
        copy_source = {
            'Bucket': 'noaa-nexrad-level2',
            'Key': key
        }
        bucket = s3.Bucket('alabama-tornadoes')
        bucket.copy(copy_source, key)

    
    def checkBucket(self,bucketName):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucketName)
        exists = True
        try:
            s3.meta.client.head_bucket(Bucket=bucketName)
        except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
        print "Buckets exists: ", exists
        return exists
    
    def GetBucketSize(self,):
        s3conn = boto.connect_s3()
        bucket = s3conn.get_bucket('noaa-nexrad-level2')
        size = 0
        for key in bucket.list():
            size += key.size
        return size
    
    
    def collect(self):
        engine = sqlalchemy.create_engine(connectionString)
        
        AlabamaDF = pd.read_sql('SELECT * FROM alabama',con = engine)
        #AlabamaDF.head()
        weather = AlabamaDF
        weather.head()
        #tornadoes = AlabamaDF[AlabamaDF["event_type"] == "Tornado"]
        weather.columns
        
        keys = []
        filenames = []
        TornadoTime = []
        VolumeTime = []
        OriginalTime = []
        IsTornado = []
        episode_ids = []
        event_ids = []
        
        for i in range((len(weather))-1):
            event = weather.iloc[i,:]
            central = pytz.timezone('US/Central')
            d = datetime.datetime.strptime(event.begin_date_time, '%d-%b-%y %H:%M:%S')
            local = central.localize(d)
            utc = local.astimezone(pytz.utc)
            station = event.wfo
            station = self.UpdateStationCode(station)
            selectedTimes = self.getSelectedTimes(station,utc)
            tornadoTime = utc.strftime('%H:%M:%S')
            if (selectedTimes !=  []):
                closestTimeStamp, formatted = self.closestTime(tornadoTime,selectedTimes)
                key = self.buildKey(station,utc,closestTimeStamp)
                filename = self.buildFileID(station,utc,closestTimeStamp)
                #if(not os.path.isfile(filename)):
                #self.GetFileFromNexradAWS(key,filename)
                #self.MoveFileFromNoaaToTornado(key, filename)
                VolumeTime.append(formatted)
                TornadoTime.append(tornadoTime)
                filenames.append(filename)
                keys.append(key)
                OriginalTime.append(d.strftime('%H:%M:%S'))
                episode_ids.append(event.episode_id)
                event_ids.append(event.event_id)
                #BeginLat.append(event.begin_lat)
                #BeginLong.append(event.begin_lon)
                if event.event_type == "Tornado":
                    IsTornado.append(1)
                    print "Tornado : ", round(float(i+1)/(len(weather)-1) * 100,2)
                else:
                    IsTornado.append(0)
                    print "Event   : ", round(float(i+1)/(len(weather)-1) * 100,2)
                    
            else:
                print "Event   : ",round(float(i)/len(weather) * 100,2)
                print "Could not find keys for: ", station, tornadoTime
            
        
        # for i in range((len(weather))-1):
        # #for i in range(100):
        #     event = weather.iloc[i,:]
        #     central = pytz.timezone('US/Central')
        #     d = datetime.datetime.strptime(event.end_date_time, '%d-%b-%y %H:%M:%S')
        #     local = central.localize(d)
        #     utc = local.astimezone(pytz.utc)
        #     station = event.wfo
        #     station = UpdateStationCode(station)
        #     selectedTimes = getSelectedTimes(station,utc)
        #     tornadoTime = utc.strftime('%H:%M:%S')
        #     if (selectedTimes !=  []):
        #         closestTimeStamp, formatted = closestTime(tornadoTime,selectedTimes)
        #         key = buildKey(station,utc,closestTimeStamp)
        #         filename = buildFileID(station,utc,closestTimeStamp)
        #         if(not os.path.isfile(filename)):
        #             #GetFileFromNexradAWS(key)
        #             print closestTimeStamp
        #             print "Volume Time: ",formatted
        #             VolumeTime.append(formatted)
        #             print "Tornado Time: ",tornadoTime
        #             TornadoTime.append(tornadoTime)
        #             filenames.append(filename)
        #             if event.event_type == "Tornado":
        #                 IsTornado.append(1)
        #                 print "Tornado : ", round(float(i)/len(weather) * 100,2)
        #             else:
        #                 IsTornado.append(0)
        #                 print "Event   : ", round(float(i)/len(weather) * 100,2)
                    
        #     else:
        #         print "Event   : ",i/len(weather)
        #         print "Could not find keys for: ", station, tornadoTime
        
        print len(VolumeTime)
        print len(TornadoTime)
        print len(IsTornado)
        print len(episode_ids)
        print len(event_ids)
        #print len(BeginLat)
        #print len(BeginLong)
        
        
        #tornadoClassification = pd.DataFrame({"Filename":filenames,"IsTornado":IsTornado,"TornadoTime":TornadoTime,"VolumeTime":VolumeTime,"OriginalTime":OriginalTime,"BeginLat":BeginLat,"BeginLong":BeginLong},columns = ["Filename","IsTornado","TornadoTime","VolumeTime","OriginalTime","BeginLat","BeginLong"])
        tornadoClassification = pd.DataFrame({"Key":keys,"Filename":filenames,"IsTornado":IsTornado,"TornadoTime":TornadoTime,"VolumeTime":VolumeTime,"OriginalTime":OriginalTime,"Episode_ID":episode_ids,"Event_ID":event_ids},columns = ["Key","Filename","IsTornado","Episode_ID","Event_ID","TornadoTime","VolumeTime","OriginalTime"])
        
        print tornadoClassification.columns
        
        tornadoClassification.IsTornado.unique()
        
        tornadoClassification.drop_duplicates().shape
        
        tornadoClassification[tornadoClassification["IsTornado"] == 1 ].head()
        
        tornadoClassification.to_sql("weather",con = engine, if_exists = "replace")
    