# -*- coding: utf-8 -*-
#############################################################################
############################## PROGRAM METADATA #############################
#############################################################################

# Last Updated by: Feng Pan
# Last Updated on: 21 April 2020
# Purpose: This script is designed to extract all the results and seperate 
#            them to soil loss, TN, and TP and then map them as tif and asc.
# Contributors: Feng made, debug, finalized the code alone.

#############################################################################
############################# INSTRUCTIONS TO RUN ###########################
#############################################################################

# Requirements: Place results_analysis.py in the working directory. THE ROOT 
#                 DIRECTORY PATH MUST NOT CONTAIN ANY SPACES. In this directory 
#                 the following must also be present:
#
# Files or directories (without extention are directories):    
# ----working directory
#               -------RESULTS
#                         --- *.csv (results file from the main code)
#               -------results analysis
#                         ---- * (generated results directory with csv file name)
#                               ---- * state_county directory
#                                    --- *.asc (results files will be generated)
#                                    --- *.tif (results files will be generated)
#
# Output explanation: all the model simulation results are updated from csv file
#                     to tif and asc files for mapping.

##############################################################################
############################## IMPORT LIBRARIES ##############################
##############################################################################

import glob, os, sys, shutil
import pandas as pd
import math
import datetime
import numpy
import gdal
import gdalconst
from datetime import timedelta
from shutil import copyfile
from multiprocessing import cpu_count, Pool, Process

#############################################################################
############################## GLOBAL VARIABLES #############################
#############################################################################

workdir = os.path.dirname(os.path.realpath(sys.argv[0]))

#############################################################################
############################## DEFINE FUNCTIONS #############################
#############################################################################
def doMapping(stcty, data, otfd):  
    
    os.chdir(otfd)    
    stctyname = [stcty]    
    judge = data['stctyname'].isin(stctyname)
    if(judge.iloc[0,] == True):
        print(stcty)
        try:
            if not os.path.exists(stcty):
                os.makedirs(stcty)
        # There are no exceptions.
        except:
            pass
        currentdir = otfd + "\\" +stcty
        stctydata = data[data['stctyname'].isin(stctyname)]                

        prcp = pd.concat([stctydata["Rowid_Colid"].astype(str),
                                    stctydata["Precipitation(mm/yr)"].astype(float)], 
                                    axis = 1, sort=False)
        runoff = pd.concat([stctydata["Rowid_Colid"].astype(str),
                            stctydata["Runoff(mm/yr)"].astype(float)], 
                            axis = 1, sort=False)
        soilloss = pd.concat([stctydata["Rowid_Colid"].astype(str),
                                stctydata["Soil Loss(t/ha)"].astype(float)], 
                                axis = 1, sort=False)
        TN = pd.concat([stctydata["Rowid_Colid"].astype(str),
                        stctydata["Total N(kg/ha)"].astype(float)], 
                        axis = 1, sort=False)
        TP = pd.concat([stctydata["Rowid_Colid"].astype(str),
                        stctydata["Total P(kg/ha)"].astype(float)], 
                        axis = 1, sort=False)

        newprcp = pd.DataFrame(prcp['Rowid_Colid'].str.split(';').tolist(), index=prcp['Precipitation(mm/yr)']).stack()
        newprcp = newprcp.reset_index([0, 'Precipitation(mm/yr)'])
        newprcp.columns = ['Precipitation(mm/yr)', 'Rowid_Colid']
        newprcp = newprcp[newprcp['Precipitation(mm/yr)'].isnull() == False]

        newrunoff = pd.DataFrame(runoff['Rowid_Colid'].str.split(';').tolist(), index=runoff['Runoff(mm/yr)']).stack()
        newrunoff = newrunoff.reset_index([0, 'Runoff(mm/yr)'])
        newrunoff.columns = ['Runoff(mm/yr)', 'Rowid_Colid']
        newrunoff = newrunoff[newrunoff['Runoff(mm/yr)'].isnull() == False]

        newsoilloss = pd.DataFrame(soilloss['Rowid_Colid'].str.split(';').tolist(), index=soilloss['Soil Loss(t/ha)']).stack()
        newsoilloss = newsoilloss.reset_index([0, 'Soil Loss(t/ha)'])
        newsoilloss.columns = ['Soil Loss(t/ha)', 'Rowid_Colid']
        newsoilloss = newsoilloss[newsoilloss['Soil Loss(t/ha)'].isnull() == False]

        newTN = pd.DataFrame(TN['Rowid_Colid'].str.split(';').tolist(), index=TN['Total N(kg/ha)']).stack()
        newTN = newTN.reset_index([0, 'Total N(kg/ha)'])
        newTN.columns = ['Total N(kg/ha)', 'Rowid_Colid']
        newTN = newTN[newTN['Total N(kg/ha)'].isnull() == False]

        newTP = pd.DataFrame(TP['Rowid_Colid'].str.split(';').tolist(), index=TP['Total P(kg/ha)']).stack()
        newTP = newTP.reset_index([0, 'Total P(kg/ha)'])
        newTP.columns = ['Total P(kg/ha)', 'Rowid_Colid']
        newTP = newTP[newTP['Total P(kg/ha)'].isnull() == False]        

        os.chdir(currentdir)
        lufn = os.path.join("D:\\APEXMP\\Maumee\\landuse","lu%s.asc" %(stcty))
        # lufn = os.path.join(workdir,"INPUTS\\landuse\\","lu%s.asc" %(stcty))
        cmd1 = 'gdal_translate -of GTiff ' + lufn + ' lu.tif'
        os.system(cmd1)

        copyfile("lu.tif", "prcp.tif")
        copyfile("lu.tif", "runoff.tif")
        copyfile("lu.tif", "soilloss.tif")
        copyfile("lu.tif", "TN.tif")
        copyfile("lu.tif", "TP.tif")

        prcptif = gdal.Open("prcp.tif", gdalconst.GA_Update)
        runofftif = gdal.Open("runoff.tif", gdalconst.GA_Update)
        soillosstif = gdal.Open("soilloss.tif", gdalconst.GA_Update)
        TNtif = gdal.Open("TN.tif", gdalconst.GA_Update)
        TPtif = gdal.Open("TP.tif", gdalconst.GA_Update)
        cols = prcptif.RasterXSize
        rows = prcptif.RasterYSize
        prcptifband = prcptif.GetRasterBand(1)
        runofftifband = runofftif.GetRasterBand(1)
        soillosstifband = soillosstif.GetRasterBand(1)
        TNtifband = TNtif.GetRasterBand(1)
        TPtifband = TPtif.GetRasterBand(1)
        prcparray = prcptifband.ReadAsArray()
        runoffarray = runofftifband.ReadAsArray()
        soillossarray = soillosstifband.ReadAsArray()
        TNarray = TNtifband.ReadAsArray()
        TParray = TPtifband.ReadAsArray()

        for rowidx in range(rows):
            for colidx in range(cols):
                
                rowid_colid = [(str(rowidx) +'_'+ str(colidx))]            

                prcpdata = newprcp[newprcp['Rowid_Colid'].isin(rowid_colid)]
                runoffdata = newrunoff[newrunoff['Rowid_Colid'].isin(rowid_colid)]
                soillossdata = newsoilloss[newsoilloss['Rowid_Colid'].isin(rowid_colid)]
                TNdata = newTN[newTN['Rowid_Colid'].isin(rowid_colid)]
                TPdata = newTP[newTP['Rowid_Colid'].isin(rowid_colid)]

                if not prcpdata.empty:
                    prcpdata = prcpdata.reset_index(drop = True)
                    prcparray[rowidx,colidx] = prcpdata.at[0, "Precipitation(mm/yr)"]
                else:
                    prcparray[rowidx,colidx] = 0.0
                if not runoffdata.empty:
                    runoffdata = runoffdata.reset_index(drop = True)
                    runoffarray[rowidx,colidx] = runoffdata.at[0, "Runoff(mm/yr)"]
                else:
                    runoffarray[rowidx,colidx] = 0.0
                if not soillossdata.empty:
                    soillossdata = soillossdata.reset_index(drop = True)
                    soillossarray[rowidx,colidx] = soillossdata.at[0, "Soil Loss(t/ha)"]
                else:
                    soillossarray[rowidx,colidx] = 0.0
                if not TNdata.empty:
                    TNdata = TNdata.reset_index(drop = True)
                    TNarray[rowidx,colidx] = TNdata.at[0, "Total N(kg/ha)"]
                else:
                    TNarray[rowidx,colidx] = 0.0
                if not TPdata.empty:
                    TPdata = TPdata.reset_index(drop = True)
                    TParray[rowidx,colidx] = TPdata.at[0, "Total P(kg/ha)"]
                else:
                    TParray[rowidx,colidx] = 0.0
                
                del prcpdata
                del runoffdata
                del soillossdata
                del TNdata
                del TPdata

        prcptifband.WriteArray(prcparray)
        prcptifband.FlushCache()
        del prcparray
        prcptif = None

        runofftifband.WriteArray(runoffarray)
        runofftifband.FlushCache()
        del runoffarray
        runofftif = None

        soillosstifband.WriteArray(soillossarray)
        soillosstifband.FlushCache()
        del soillossarray
        soillosstif = None

        TNtifband.WriteArray(TNarray)
        TNtifband.FlushCache()
        del TNarray
        TNtif = None

        TPtifband.WriteArray(TParray)
        TPtifband.FlushCache()
        del TParray
        TPtif = None

        cmd2 = 'gdal_translate -of AAIGrid prcp.tif prcp.asc'
        cmd3 = 'gdal_translate -of AAIGrid runoff.tif runoff.asc'
        cmd4 = 'gdal_translate -of AAIGrid soilloss.tif soilloss.asc'
        cmd5 = 'gdal_translate -of AAIGrid TN.tif TN.asc'
        cmd6 = 'gdal_translate -of AAIGrid TP.tif TP.asc'
        os.system(cmd2)
        os.system(cmd3)
        os.system(cmd4)
        os.system(cmd5)
        os.system(cmd6)

        del stctydata
        del prcp, 
        del runoff
        del soilloss
        del TN
        del TP
        del newprcp
        del newrunoff
        del newsoilloss
        del newTN
        del newTP
        del judge
def main():

    now = datetime.datetime.now()
    tstart = str(now.year) + "-" + str(now.month) + "-" + str(now.day) + "-" + str(now.hour) + "-" + str(now.minute) + "-" + str(now.second)

    # Get the result state list
    stlst = []
    fnlst = []
    resultslst = glob.glob("%s/*.csv" %(workdir+"\\RESULTS")) 
    for sid in range(len(resultslst)):
        fnlst.append(os.path.split(resultslst[sid])[-1][:-4])
        stlst.append(fnlst[sid].split("_")[0])

    # Get state and county list
    stctys = []
    # stctys = glob.glob("%s/*.shp" %("D:\\APEXMP\\Maumee\\county"))  
    stctys = glob.glob("%s/*.shp" %(workdir+"\\INPUTS\\county"))
    for cid in range(len(stctys)):
        stctys[cid] = os.path.split(stctys[cid])[-1][:-4]

    # Create the the state-county dictionary with state names as keys and state_county list as values
    stctydict = {stlstkey: [stcty for stcty in stctys if stcty.startswith(stlstkey)] for stlstkey in stlst}

    for stidx in range(len(list(stctydict.keys()))):
        statename = list(stctydict.keys())[stidx]
        stctylst = stctydict[statename]   
        # create the state folder
        try:
            if not os.path.exists(workdir+"\\results analysis\\"+fnlst[stidx]):
                os.makedirs(workdir+"\\results analysis\\"+fnlst[stidx])
        # There are no exceptions.
        except:
            pass
        
        otfd = ""
        otfd = workdir+"\\results analysis\\"+fnlst[stidx]

        # get all the results for state
        f = pd.read_csv(resultslst[stidx])
        nodata = [-999]
        data = f[f['Precipitation(mm/yr)'].isin(nodata) == False]    
        data['stctyname'] = data[['State', 'County']].agg('_'.join, axis=1)
        
        # for stctyidx in range(len(stctylst)):
        m_list = []

        for i in range(len(stctylst)):
            m_list.append((stctylst[i], data, otfd))  
            # doMapping(stctylst[i], data, otfd)
        
        # Clear the terminal output.
        print("\n\r" * 30)

        # Determine a suitable number of cores to use.
        cores = cpu_count()
        nworkers_75 = math.floor(cores/4*3)
        nworkers_4b = cores - 4
        nworkers_min = 1
        nworkers_max = 32
        nworkers = int(max(nworkers_75, nworkers_4b, nworkers_min))
        nworkers = int(min(nworkers, nworkers_max))

        # Create the worker pool to map.
        sys.stdout.flush()

        tps1 = datetime.datetime.now()
        print("\n\rParallel mapping of " + statename + " has begun at " + str(tps1) + " ...\n\r")
        workers = Pool(nworkers)
        for i in range(len(m_list)):
            workers.apply_async(doMapping, args=m_list[i])
        workers.close()
        workers.join()
        tps2 = datetime.datetime.now()    
        print("\n\rParallel mapping of " + statename + " has completed at " + str(tps2) + " .\n\r")

        del data
        del f

    now = datetime.datetime.now()
    tend = str(now.year) + "-" + str(now.month) + "-" + str(now.day) + "-" + str(now.hour) + "-" + str(now.minute) + "-" + str(now.second)

    # Print the total start and end times.
    print("\n\rStart time: " + tstart)
    print("\n\rEnd time:   " + tend)   

    # Exit upon completion.
    sys.exit()
#############################################################################
################################ RUN PROGRAM ################################
#############################################################################
if __name__ == '__main__':
    main()