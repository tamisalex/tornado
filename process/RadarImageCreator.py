'''
Created on Jan 5, 2017

@author: alexandertam
'''
import os
import sqlalchemy
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import numpy.ma as ma
import pyart
import time
import math
# import singledop
from copy import deepcopy
import boto3
import tempfile
 
from csu_radartools import (csu_misc, csu_kdp)
 
import gzip
import os

from psycopg2.extensions import register_adapter, AsIs


connectionString = "postgresql://axt4989:ekye6emNonagon9@tamisalex.cbpjsu8olcvg.us-east-1.rds.amazonaws.com:5432/weather"


class RadarImageCreator(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''a
        Constructor
        '''
        
    def extract_unmasked_data(self,radar, field, bad=-32768):
        """Simplify getting unmasked radar fields from Py-ART"""
        return radar.fields[field]['data'].filled(fill_value=bad)


    def add_field_to_radar_object(self,field, radar, field_name='FH', units='unitless', 
                                  long_name='Hydrometeor ID', standard_name='Hydrometeor ID',
                                  dz_field='ZC'):
        """
        Adds a newly created field to the Py-ART radar object. If reflectivity is a masked array,
        make the new field masked the same as reflectivity.
        """
        fill_value = -32768
        masked_field = np.ma.asanyarray(field)
        masked_field.mask = masked_field == fill_value
        if hasattr(radar.fields[dz_field]['data'], 'mask'):
            setattr(masked_field, 'mask', 
                    np.logical_or(masked_field.mask, radar.fields[dz_field]['data'].mask))
            fill_value = radar.fields[dz_field]['_FillValue']
        field_dict = {'data': masked_field,
                      'units': units,
                      'long_name': long_name,
                      'standard_name': standard_name,
                      '_FillValue': fill_value}
        radar.add_field(field_name, field_dict, replace_existing=True)
        return radar
    
    
    # In[17]:
    

        
    def generateImage(self,filename,radar, isTornado):
        display = pyart.graph.RadarDisplay(radar)
        #fig = plt.figure(figsize=(6, 5))
    
        # plot super resolution reflectivity
        #ax = fig.add_subplot(111)
        display.plot('DZ_qc', 0, title='NEXRAD Reflectivity',
                     vmin=-32, vmax=64, colorbar_flag=False)
        #display.plot_range_ring(radar.range['data'][-1]/1000., ax=ax)
        #display.set_limits(xlim=(-500, 500), ylim=(-500, 500), ax=ax)
        filename = filename.split('.')[0]
        plt.axis('off')
        plt.title("")
        #ax.get_xaxis().set_visible(False)
        #plt.clear_output(wait=True)
        #ax.axes.get_yaxis().set_visible(False)
        print os.getcwd()
        if isTornado:
            plt.savefig('./assets/tornadoes/'+filename+'_velo',frameon=None,bbox_inches='tight',pad_inches=0)
        else:
            plt.savefig('./assets/nottornadoes/'+filename,frameon=None,bbox_inches='tight',pad_inches=0)
        #plt.show()
        plt.close()
    
    
    def addapt_numpy_float32(self, numpy_float32):
        return AsIs(numpy_float32)

    def process(self):
        
        very_start = time.clock()
        
        register_adapter(np.float32, self.addapt_numpy_float32)
        
        engine = sqlalchemy.create_engine(connectionString)
        weather = pd.read_sql('SELECT * FROM weather',con = engine)
        #weather = weather[weather["IsTornado"] == 1]
        weather.drop_duplicates(["Filename","TornadoTime"])
        weather.reset_index()
        #del weather["index"]


# In[23]:

        data = weather.copy(deep=True)
        data = data[~data["Filename"].str.contains("V03")].reset_index()

        s3 = boto3.resource('s3')
        bucket = s3.Bucket('noaa-nexrad-level2')

        lenOfData = len(data)-1
        #for i in range(lenOfData):
        for i in range(0,lenOfData):
        
            
        
            print "Percent Complete: ", round((i+1)/float(lenOfData)*100,2)
            print "Reading "+ data.loc[i,"Filename"] + " ..."
            radar = ""
            try:
                bucket.download_file(data.ix[i,"Key"], "./assets/"+data.ix[i,"Filename"])
                radar = pyart.io.read_nexrad_archive("./assets/"+ data.ix[i,"Filename"])
                
                print "Cleaning ..."
                start = time.clock()
                
                #dzN = radar.get_field(0,"reflectivity")
                #drN = radar.get_field(0,"differential_reflectivity")
                #dpN = radar.get_field(0,"differential_phase")
                #dvN = radar.get_field(1,"velocity")
                
                if (radar != ""):
                    dzN = self.extract_unmasked_data(radar, 'reflectivity')
                    drN = self.extract_unmasked_data(radar, 'differential_reflectivity')
                    dpN = self.extract_unmasked_data(radar, 'differential_phase')
                #dvN = extract_unmasked_data(radar, 'velocity')
                
                rng2d, az2d = np.meshgrid(radar.range['data'], radar.azimuth['data'])
                
                kdN, fdN, sdN = csu_kdp.calc_kdp_bringi(
                dp=dpN, dz=dzN, rng=rng2d/1000.0, thsd=12, gs=250.0, window=5)
                
                #generate insect mask from reflectivity and differential reflectivity
                insect_mask = csu_misc.insect_filter(dzN, drN)
                sdp_mask = csu_misc.differential_phase_filter(sdN, thresh_sdp=20)
                
                #apply mask to respective fields
                bad = -32768
                dz_insect = 1.0 * dzN
                dz_insect[insect_mask] = bad
                dz_sdp = 1.0 * dzN
                dz_sdp[sdp_mask] = bad
                
                #join masks
                new_mask = np.logical_or(insect_mask, sdp_mask)
                
                #copy reflectivity and apply joined mask
                dz_qc = 1.0 * dzN
                dz_qc[new_mask] = bad
                
                #despeckle then add new field to radar object
                mask_ds = csu_misc.despeckle(dz_qc, ngates=15)
                dz_qc[mask_ds] = bad
                radar = self.add_field_to_radar_object(dz_qc, radar, field_name='DZ_qc', units='dBZ', 
                                              long_name='Reflectivity (Combo Filtered)',
                                              standard_name='Reflectivity (Combo Filtered)', 
                                              dz_field='reflectivity')
                
                reflect_mask = radar.fields["DZ_qc"]["data"].mask[0:720]
                radar.fields["velocity"]["data"][720:1440].mask = reflect_mask
                #radar.fields["velocity"]["data"][720:1440]
                
                
                #dz_mask = getattr(radar.fields['DZ_qc']['data'], 'mask')
                #dv_mask = getattr(radar.fields['velocity']['data'], 'mask')
                #combined_mask = np.logical_or(dz_mask, dv_mask)
                
                #ve = deepcopy(radar.fields['velocity']['data'])
                #radar.add_field_like('velocity', 'DV_qc', ve, replace_existing=True)
                #setattr(radar.fields['DV_qc']['data'], 'mask', combined_mask)
            
                #dv_qc = 1.0 * dvN
                #dv_qc[new_mask] = 
                #mask_ds = csu_misc.despeckle(dv_qc, ngates=15)
                #dv_qc[mask_ds] = bad
            
            
                #radar.get_field(0,"reflectivity")
            
                #radar = add_field_to_radar_object(dv_qc, radar, field_name='DV_qc', units='m/s', 
                #                               long_name='Velocity (Combo Filtered)',
                #                               standard_name='Velocity (Combo Filtered)', 
                #                               dz_field='velocity')
                #end = time.clock()
                #print "Time: ", end - start
                #print "Saving ..."
                #start = time.clock()
                print data.ix[i,"IsTornado"]
                self.generateImage(data.ix[i,"Filename"],radar, data.ix[i,"IsTornado"])
                #self.cleaned_radar_image(data.ix[i,"Filename"],radar, sweep=0, var1='DZ_qc', vmin1=-80, vmax1=80, cmap1='bwr', units1='m/s',xlim=[-300,300], ylim=[-300,300])
            
                
                #sqlStatement = "SELECT latitude,longitude FROM locations WHERE episode_id =" + str(event.Episode_ID) + " AND event_id = " + str(event.Event_ID)
                
                #tornadoLoc = pd.read_sql(sqlStatement,con = engine)
                    
                
                #box = get_bounding_box(tornadoLoc.latitude[0], tornadoLoc.longitude[0], 120)
                
                #print box.min_lat
                #print box.min_lon
                #print box.max_lat
                #print box.max_lon
                #print "---------"    
                #print tornadoLoc.latitude[0]
                #print tornadoLoc.longitude[0]
                
                #cleaned_radar_map_image(event.Filename, radar, sweep=1, var='velocity', vmin=-80, vmax=80,cmap='bwr', units='m/s', minLat = box.min_lat, minLong = box.min_lon, maxLat = box.max_lat, maxLong = box.max_lon,tornadoLat = tornadoLoc.latitude[0], tornadoLong = tornadoLoc.longitude[0])
                
     
            
                end = time.clock()
                print "Time: ", end - start
                print "-----------------------------"
                
                os.remove("./assets/"+ data.ix[i,"Filename"])
            except:
                print "Error reading file!"
            
        very_end = time.clock()
            
        print "Total Time: ", (very_end - very_start)
