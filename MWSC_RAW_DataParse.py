#Scintillometer RawParse
#For use with UofU Two Wavelength Scintillometer System
#Version 1.0

#Loads ASCII RAW file outputs from MWSC software
#Separate the file into a more readable format
#Each scintillometer 1000Hz signal is given it's own file
#Meteorological data from the Vaisala weather station given own file
#Each file has timestamp each second

#Each hourly file takes ~15s to run
#Processing time makes up about half of total time
#TO DO: make this work on multiple CPUS to speed up processing time

import re
import timeit
import os
import glob
import multiprocessing as mp

#------------------------------------------------------------
#------------------------------------------------------------
#Sort Data Function
#Use:
	#data -> string of data blocks with no additional header information
			#at the top of the file. Headers within data block okay
	#dataRegEx -> Regular expression that defines the entire data block
			#including any imbedded header information
def sortData(data, dataRegEx):
	done = 0		#While loop variable
	cntr = 0		#counter variable for loop

	#Initialize temporary variables
	tmpMet1 = []
	tmpLAS1 = []
	tmpLAS2 = []
	tmpMWS = []
	while done!=1:
		#Collect a data block from data input
		#Header information is removed from data
		dataBlock = dataRegEx.match(data)
		
		#If dataBlock is not empty
		if dataBlock:
			#Remove white space from MetData
			Met = re.sub(r"\s+", '', dataBlock.group('MetData'))
			
			#Move Met data to temporary variable
			tmpMet1.append(Met+'\n')
		
			#Collect Date information
			date = Met[:18]
			
			#Write scintillometer data to temporary variables with date information
			tmpLAS1.append(date+re.sub(r"\s+", ',', dataBlock.group('LAS1'))[:]+'\n')
			tmpLAS2.append(date+re.sub(r"\s+", ',', dataBlock.group('LAS2'))[:]+'\n')
			tmpMWS.append(date+re.sub(r"\s+", ',', dataBlock.group('MWS'))[:]+'\n')
			
			#remove the already collected datablock from data
			data = data[dataBlock.end(0)+1:]
			
			cntr += 1
		else:
			done = 1
			print('Finished with %d Samples' %(cntr))

	#Combine the temporary lists into string variables and return their values
	LAS1 = ''.join(tmpLAS1)
	LAS2 = ''.join(tmpLAS2)
	MWS = ''.join(tmpMWS)
	Met1 = ''.join(tmpMet1)
	return(LAS1, LAS2, MWS, Met1)
#------------------------------------------------------------
#------------------------------------------------------------

#############
#------------------------------------------------------------
#User Inputs

#Number of chunks to split file into
numChunks = 20

#Regular exprssion format for number of samples 
#"<1+ digits> # number of samples"
progNumSample = re.compile(r"(?P<NumSamples>\d+)\s+(?P<Comment># number of samples)")

#Regular expression format for each block of data
	#Met Header line
	#Met Data Line
	#LAS Aperture 1 start line
	#20 lines of LAS Aperture 1 Data
	#LAS Aperture 2 start line
	#20 lines of LAS Aperture 2 Data
	#MWS start line
	#20 lines of MWS Data
progBlock = re.compile(r"(?P<MetHeader># Ye.+)\n(?P<MetData>.+)\n(?P<LAS1_Start># 1000.+Aperture 1.+)\n(?P<LAS1>(.+\n){19}.+)\n(?P<LAS2_Start># 1000.+Aperture 2.+)\n(?P<LAS2>(.+\n){19}.+)\n(?P<MWS_Start># 1000 MWS.+)\n(?P<MWS>(.+\n){19}.+)", re.M)

#RAW File extension
#Format: '*.EXT'
rawExt = '*.RAW.ASC'

#File format
#Format: YYMMDD_hhmmss
progFileName = re.compile(r"(?P<Year>\d{2})(?P<Month>\d{2})(?P<Day>\d{2})_(?P<Hour>\d{2})(?P<Minute>\d{2})(?P<Second>\d{2})")

#Main File Directory
#must end with '/'
fileDir = "G:/Alexei/Data/Oregon_2016/Scintillometer/RAW"
#Save Directory
saveDir = "G:/Alexei/Data/Oregon_2016/Scintillometer/RAW_Convert"

#End User Inputs
#------------------------------------------------------------
#############

#########
#Start timer for overall run time
startOverall = timeit.default_timer()
#########

#Check that SaveDir exists and make it otherwise
if not os.path.exists(saveDir):
	os.makedirs(saveDir)
	#Since main directory does not exist, subdirectories cannot exist
	#Make subdirectories for each data product
	#If these directoty names are edited, must also be edited at
	#file name variables lower down
	os.makedirs(saveDir+'/LAS1')		#LAS Aperture 1
	os.makedirs(saveDir+'/LAS2')		#LAS Aperture 2
	os.makedirs(saveDir+'/MWS')			#MWS
	os.makedirs(saveDir+'/Met')			#Met Data

for fileName in glob.glob(os.path.join(fileDir, rawExt)):
	#########
	#Start timer for file run time
	start = timeit.default_timer()
	#########
	
	workingNumChunks = numChunks
	
	print('\nWorking on file...\n'+fileName+'\n')
	
	#Get date information from filename
	tmp = progFileName.search(fileName)
	fileDate = tmp.group(0)
	
	#Final File Names and Locations relative to saveDir
	#must start with '/'
	LAS1fileName = '/LAS1/LAS1_'+fileDate+'.dat'
	LAS2fileName = '/LAS2/LAS2_'+fileDate+'.dat'
	MWSfileName = '/MWS/MWS_'+fileDate+'.dat'
	MetfileName = '/Met/Met_'+fileDate+'.dat'

	#Load hourly file contents into memory
	with open(fileName, "rt", encoding="8859") as dataFile:
		dataContent = dataFile.read()

	#Find Number of Samples that exist in the file
	tmp = progNumSample.search(dataContent)
	numSamples = int(tmp.group('NumSamples'))

	#Remove all header lines from dataContent
	dataContent = dataContent[tmp.end(0)+1:]

	#Separate dataContent into numChunks chunks
	#Check if there is an odd amount of data that results
	#in a smaller chunk remaining
	dataChunk = []
	if not (numSamples % workingNumChunks)==0:
		chunkSize = numSamples // workingNumChunks
		#Remaining chunkSize is smaller
		chunkSizeLast = numSamples % workingNumChunks
		
		#Increase numChunks by one and set flag
		workingNumChunks += 1
		chunkFlag = True
	else:
		chunkSize = numSamples // workingNumChunks
		chunkFlag = False

# #	#Regular expression for a datachunk of size chunkSize
# #		#Same as above except repeated 
# #	progChunk = re.compile(r"(?P<DataBlock>(# Ye.+\n(.+\n){64}){"+str(chunkSize)+"})", re.M)

	#Separate hurly data file into chunks
	for i in range(0, workingNumChunks+1):
		#If there exists a smaller last chunk
		if chunkFlag and i==workingNumChunks:
			progChunk = re.compile(r"(?P<DataBlock>(# Ye.+\n(.+\n){64}){"+str(chunkSizeLast)+"})", re.M)
			tmp = progChunk.search(dataContent)
		#Normal sized chunks
		else:
			tmp = progChunk.search(dataContent)		
		if tmp:
			dataChunk.append(tmp)
			dataContent = dataContent[tmp.end(0):]

	#Create separate files for Met Data, LAS1, LAS2, and MWS
	fileMet = open(saveDir+MetfileName, "w+")
	fileLAS1 = open(saveDir+LAS1fileName, "w+")
	fileLAS2 = open(saveDir+LAS2fileName, "w+")
	fileMWS = open(saveDir+MWSfileName, "w+")

	#Process through data chunks
	for i in dataChunk:
		tmp1, tmp2, tmp3, tmp4 = sortData(i.group('DataBlock'), progBlock)
		
		print("Writing to files...")
		fileMet.write(tmp4)
		fileLAS1.write(tmp1)
		fileLAS2.write(tmp2)
		fileMWS.write(tmp3)

	#Close files
	fileMet.close()
	fileLAS1.close()
	fileLAS2.close()
	fileMWS.close()
		
	#########	
	stop = timeit.default_timer()
	print('File Time: ', round(stop - start, 2)) 
	#########

#########	
stopOverall = timeit.default_timer()
print('Total Time: ', round(stopOverall - startOverall, 2)) 
#########
