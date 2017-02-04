#from process import StormDataEventCollector
#from process import RadarDataCollector
#from process import RadarDataProcessor
from process import ModelBuilder


def handler(event, context):
    print "Collecting Storm Data Events..."
     
    #sdec = StormDataEventCollector.StormDataEventCollector()
    #sdec.collect()
    
    print "Collecting Radar Data..."
     
    #rdc = RadarDataCollector.RadarDataCollector()
    #rdc.collect()
    
    print "Processing Radar Data..."
    
    #rdp = RadarDataProcessor.RadarDataProcessor()
    #rdp.process()
    
    print "Building Model"
    
    mb = ModelBuilder.ModelBuilder()
    mb.buildModel()
    
    print "Finished"
