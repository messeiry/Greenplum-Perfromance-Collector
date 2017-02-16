#! /usr/bin/python

import psycopg2, sys, datetime, time
#from prettytable import PrettyTable
from socket import *

# Define Connection to Master GreenPlum DB
gpMasterIP = "10.222.3.4"
gpName = "GreenPlumProdDB"
gpDbName = "gpperfmon"
gpUser = "oss_monitor_v2"
gpPassword = "monitor_v2@123"

# General vars
serverHost = 'localhost'            # servername is localhost
serverPort = 2101                   # use arbitrary port > 1024 (backend port)

pullingInterval = 5		    # in minutes

doSendToBackend = True
printData = False

s = None



#============================================================================================
# SEND DATA to Socket
#============================================================================================ 

def sendToBackend(dataToSend):
	global s
	if not s:
		s = socket(AF_INET,SOCK_STREAM)    # create a TCP socket
		s.connect((serverHost,serverPort)) # connect to server on the port
	s.send(dataToSend.encode('utf-8')) # send the data

def sendStr(data):
	if doSendToBackend: sendToBackend(data)
	if printData: print data
	
def toRawData(timestamp,group,var,val,device,devtype,unit,name,source,other):
	result =  "+r\t"+timestamp+"\t"+group+"\t"+var+"\t"+val+"\tdevice="+device+"\tdevtype="+devtype+"\tunit=""\tname="+name+"\tsource="+source
	if other:
		result += "\t"+other
	result+= "\n"
	return result

def send(timestamp,group,var,val,device,devtype,unit,name,source,other):
	sendStr(toRawData(timestamp,group,var,val,device,devtype,unit,name,source,other))


#def toUnixTime(dt):
#    epoch = datetime.datetime.utcfromtimestamp(0)
#    delta = dt - epoch
#    return delta.total_seconds()

def toUnixTime(ts):
	tsStr= str(ts)
	return str(int(time.mktime(time.strptime(tsStr,"%Y-%m-%d %H:%M:%S"))))

	
#============================================================================================
# Get Data
#============================================================================================ 


def getData(sql):
	conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (gpMasterIP, gpDbName, gpUser, gpPassword)
	conn = psycopg2.connect(conn_string)
	print "Successfully Connected to ----> GreenPlum DB : %s" %gpDbName

	# Fetching Sample records
	cursor = conn.cursor()
	cursor.execute(sql)
	colnames = [desc[0] for desc in cursor.description]
	colCount = len(colnames)
	records = cursor.fetchall()

	return colCount, colnames, records

def doQuery(sql,indexOfTimestamp, indexOfHostname ,indexOfPart, propertyLockup, indexOfProperties,indeciesIgnored,parttype ):
	colCount, colnames, records = getData(sql)

	print parttype + " ==> " + str(len(records)) + " records"


	for index, colname in enumerate(colnames):
		if (colname in propertyLockup):
			indexOfProperties.append(index)

	for index, record in enumerate(records):
		props = []
		timestamp = None
		device = gpName
		part = ""
		hostname = ""
		for ind, col in enumerate(record):
			if (ind in indexOfProperties):
				props.append([ind,col])
			if ind == indexOfTimestamp:
				timestamp = col
			if ind == indexOfPart:
				part = col
			if ind == indexOfHostname:
				hostname = col
		timestamp = toUnixTime(timestamp)
		for ind, val in enumerate(record):
			if (ind not in indexOfProperties and ind not in indeciesIgnored):
				name =  colnames[ind].replace("_","")
				var = "GreenPlumDB" + device + hostname + parttype + part + name 
				propStr = "part=" + part + "\tparttype=" + parttype + "\thostname=" + hostname
				for index,propVal in props:
					if propStr != "": propStr += "\t"
					propStr += colnames[index] + "=" + str(propVal).replace("\t", "")
					propStr = propStr.replace("_","")
				val = str(val)
				#send(timestamp,group,var,val,device,devtype,unit,name,source,other):
				send(timestamp, "groupGP", var, val, device, "GreenPlumDB", "", name, "GreenPlum_Perf", propStr)



def doSystemHistory():
	propertyLockup = []
	indexOfProperties = []
	indeciesIgnored = [0,1]
	sql = "SELECT * FROM system_history where ctime > (current_timestamp - interval '%s')" % (pullingInterval * 60);

	doQuery(sql,0, 1, -1, propertyLockup, indexOfProperties,indeciesIgnored, "System" )


def doQueriesHistory():
	propertyLockup = []
	#indeciesIgnored = [0,5,18]
	#indexOfProperties = [1,4,7,8,9,10,17,19,20,21]
	indeciesIgnored = [0,5,17,18]
	indexOfProperties = [1,4,7,8,9,10,19,20,21]
	#sql = "SELECT * FROM queries_history where ctime > (current_timestamp - interval '%s')" % (pullingInterval * 60);
	sql = "SELECT * FROM queries_history where ctime > (current_timestamp - interval '%s')" % (150 * 60);

	doQuery(sql,0, -1 ,5, propertyLockup, indexOfProperties,indeciesIgnored, "query" )

def doDatabaseHistory():
	propertyLockup = []
	indeciesIgnored = [0]
	indexOfProperties = []
	sql = "SELECT * FROM database_history where ctime > (current_timestamp - interval '%s')" % (pullingInterval * 60);

	doQuery(sql,0, -1 ,-1, propertyLockup, indexOfProperties,indeciesIgnored, "database" )

def doDiskspaceHistory():
	propertyLockup = []
	indeciesIgnored = [0,1,2]
	indexOfProperties = []
	sql = "SELECT * FROM diskspace_history where ctime > (current_timestamp - interval '%s')" % (pullingInterval * 60);

	doQuery(sql,0, 1 ,2, propertyLockup, indexOfProperties,indeciesIgnored, "filesystem" )

def doFilerepHistory():
	propertyLockup = []
	indeciesIgnored = [0,3]
	indexOfProperties = [5]
	sql = "SELECT * FROM filerep_history where ctime > (current_timestamp - interval '%s')" % (pullingInterval * 60);

	doQuery(sql,0, 3 ,-1, propertyLockup, indexOfProperties,indeciesIgnored, "filerep" )

def doSegmentHistory():
	propertyLockup = []
	indeciesIgnored = [0,2]
	indexOfProperties = []
	sql = "SELECT * FROM segment_history where ctime > (current_timestamp - interval '%s')" % (pullingInterval * 60);

	doQuery(sql,0, 2 ,-1, propertyLockup, indexOfProperties,indeciesIgnored, "segment" )


def main():
	doSystemHistory()
	doQueriesHistory()
	doDatabaseHistory()
	doDiskspaceHistory()
	doFilerepHistory()
	doSegmentHistory()
	


if __name__ == "__main__":
	main()