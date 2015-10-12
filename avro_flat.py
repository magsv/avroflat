import avro.protocol as avpr
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import os
import json
import logging
import sys

def main(avprFile,outputFolder):
	if not os.path.exists(outputFolder):
		os.makedirs(outputFolder)
	schema_formats = avpr.parse(open(avprFile, "r").read()) 
	sorted([(v.type, k) for k, v in schema_formats.types_dict.items()])
	for k, v in schema_formats.types_dict.items():
		logging.info('Processing key:'+str(k))
		checkAndProcessSchema(k,v.to_json(),outputFolder)
	

def checkAndProcessSchema(schemaName,jsonData,outputDir):
	#check if we have a protocol definition on the schema
	logging.info('Found protocol schema:'+schemaName)
	writeJsonToFile(schemaName,outputDir,jsonData,jsonData['namespace'])

def writeJsonToFile(schemaName,outputDir,data,namespace):
	logging.info("Writing schema with name:"+schemaName+" and namespace:"+namespace)
	outputFolder=buildFolderNameForSchema(outputDir,namespace)
	#check if it exists otherwise create it
	if not os.path.exists(outputFolder):
		os.makedirs(outputFolder)
	outFileName=os.path.join(outputFolder,schemaName+".avsc")
	outFile = open(outFileName, "w")
	outFile.write(json.dumps(data, indent=4, sort_keys=True))
	outFile.close()
	logging.info('Wrote data to:'+outFileName)

def buildFolderNameForSchema(outputDir,namespace):
	#replace all dots with slashes in namespace
	outputNameSpaceFolder=namespace.replace(".",os.sep)
	return os.path.join(outputDir,outputNameSpaceFolder)

if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	total = len(sys.argv)
	
	if total==3:
		avprFile=sys.argv[1]
		outputFolder=sys.argv[2]
		main(avprFile,outputFolder)
	else:
		
		logging.error("Missing required input parameters: <input avpr file> <output folder>")