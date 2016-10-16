import logging

from spyne import Application, srpc, ServiceBase, Iterable, UnsignedInteger,String,Double,Unicode

from spyne.protocol.json import JsonDocument
from spyne.protocol.http import HttpRpc
from spyne.server.wsgi import WsgiApplication
import urllib2
import urllib
import json
import operator
import datetime


# https://api.spotcrime.com/crimes.json?lat=37.334164&lon=-121.884301&radius=0.02&key=.

class CrimeCheckerService(ServiceBase):
    @srpc(Double,Double,Double, _returns=Iterable(String))
    def checkcrime(lat,lon,radius):
	data = {}
	data['lat']=lat
	data['lon']=lon
	data['radius']=radius  
	data['key']='.'      
	url_values = urllib.urlencode(data)
	
	url ='https://api.spotcrime.com/crimes.json'
	full_url = url +'?'+url_values
	#call the api
	data = urllib2.urlopen(full_url)
	the_page = data.read()
	json_data = json.loads(the_page)
	dataCrimes=json_data['crimes']
	#initialize variables
	result = {}
	totalCrimeCount  = 0
	crimeTypeCountDict = {}
	eventTimeCountDict = {}
	mostDangerousStreetDict ={}
	crimeTypeCountList = []
	eventTimeCountList = []
	mostDangerousStreetList = []

	# total crime count
	totalCrimeCount = len(dataCrimes)

	for crime in dataCrimes:
		crimeTypeCountList.append(crime['type'])
		mostDangerousStreetList.append(crime['address'])
		eventTimeCountList.append(crime['date'])

	for i in crimeTypeCountList:
		crimeTypeCountDict[i]= crimeTypeCountList.count(i)
	
	trimmeddangerousStreets = []
	for street in mostDangerousStreetList:
		if '& ' in street:
			streetName =street.split('& ')
			trimmeddangerousStreets.append(streetName[0]+' '+streetName[1]) 
		elif 'BLOCK OF ' in street:
			streetName = street.split('BLOCK OF ')
			trimmeddangerousStreets.append(streetName[1])
		elif 'BLOCK BLOCK ' in street:	
			streetName = street.split('BLOCK BLOCK ')
			trimmeddangerousStreets.append(streetName[1])
		elif 'AND ' in street:
			streetName = street.split('AND ')
			trimmeddangerousStreets.append(streetName[1])
		else:
			trimmeddangerousStreets.append(street)
			
	for st in trimmeddangerousStreets:
		mostDangerousStreetDict[st] = mostDangerousStreetList.count(st)

	top3dangerousStreets = dict(sorted(mostDangerousStreetDict.iteritems(), key=operator.itemgetter(1), reverse=True)[:3]).keys()	


	upto3am, upto6am , upto9am , upto12pm , upto3pm , upto6pm , upto9pm , upto12am = 0,0,0,0,0,0,0,0
	for timedate in eventTimeCountList:
		i = timedate.split(' ',1)
		ti = datetime.datetime.strptime(i[1],'%I:%M %p').time()
		if (ti>=datetime.time(00,01) and ti <= datetime.time(03,00)):
			upto3am = upto3am + 1
		elif(ti>=datetime.time(03,01) and ti <= datetime.time(06,00)):
			upto6am = upto6am + 1
		elif(ti>=datetime.time(06,01) and ti <= datetime.time(9,00)):				
			upto9am = upto9am + 1		
		elif(ti>=datetime.time(9,01) and ti <= datetime.time(12,00)):
			upto12pm = upto12pm + 1
		elif(ti>=datetime.time(12,01) and ti <= datetime.time(15,00)):
			upto3pm = upto3pm + 1
		elif(ti>=datetime.time(15,01) and ti <= datetime.time(18,00)):				
			upto6pm = upto6pm + 1		
		elif(ti>=datetime.time(18,01) and ti <= datetime.time(21,00)):
			upto9pm = upto9pm + 1
		else:
			upto12am = upto12am + 1	
					
		
	eventTimeCountDict["12:01am-3am"] = upto3am
	eventTimeCountDict["3:01am-6am"] = upto6am
	eventTimeCountDict["6:01am-9am"] = upto9am
	eventTimeCountDict["9:01am-12noon"] = upto12pm
	eventTimeCountDict["12:01pm-3pm"] = upto3pm
	eventTimeCountDict["3:01pm-6pm"] = upto6pm
	eventTimeCountDict["6:01pm-9pm"] = upto9pm
	eventTimeCountDict["9:01pm-12midnight"] = upto12am

	result["total_crime"] = totalCrimeCount
	result["the_most_dangerous_streets"] = top3dangerousStreets
	result["crime_type_count"] = crimeTypeCountDict
	result["event_time_count"] = eventTimeCountDict
		
	print json.dumps(result, indent=4, sort_keys=False)
	yield result
	

if __name__ == '__main__':
    # Python daemon boilerplate
    from wsgiref.simple_server import make_server

    logging.basicConfig(level=logging.DEBUG)

    application = Application([CrimeCheckerService], 'spyne.examples.CrimeCheck.http',
        in_protocol=HttpRpc(validator='soft'),

        out_protocol=JsonDocument(ignore_wrappers=True),
    )

    wsgi_application = WsgiApplication(application)

    # More daemon boilerplate
    server = make_server('127.0.0.1', 8000, wsgi_application)

    logging.info("listening to http://127.0.0.1:8000")
    logging.info("wsdl is at: http://localhost:8000/?wsdl")

    server.serve_forever()
