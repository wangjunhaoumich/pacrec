import datetime
import requests
import json
from datetime import timedelta
import graphlab
graphlab.product_key.set_product_key('4250-1731-8151-FA0A-333F-24FD-ADEE-49C9')
from random import randint
from multiprocessing import Pool
import json
import time

# proxyframe = graphlab.load_sframe('/Users/juwang/Desktop/final hotel crawler with proxy and prvate browsing/proxylist')
# proxyframelen = len(proxyframe)

def dictToFrame(inputdict):
    return graphlab.SFrame(inputdict).unpack('X1',column_name_prefix='')

# def createProxyDict():
#     return proxyframe[randint(0,proxyframelen)]

def generateRegionID(place):

    link = 'http://suggestch.expedia.com/api/v4/resolve/{}?regiontype=multicity&format=json&client=App.Server.FindYourTrip'.format(place)
    rest = requests.get(link)
    json_str = json.loads(rest.content)
    content = json_str['sr'][0]['gaiaId']
    return content


def datetime_range(start, end, delta):

    current = start
    if not isinstance(delta, timedelta):
        delta = timedelta(**delta)
    while current < end:
        yield current
        current += delta


# create package information for URL generation
def generatePackageURLs(origin,destination,defaultstartdays=7,forwarddays=7,intervaldays=7,tripdays=5,roomnumber=1):

    start = datetime.datetime.today() + timedelta(days=defaultstartdays)
    end = start + timedelta(days=forwarddays)
    datelist = []
    #this unlocks the following interface:
    for dt in datetime_range(start, end, {'days': intervaldays}):
        datelist.append((dt,dt+timedelta(days=tripdays)))

    trips = []
    originId = generateRegionID(origin)
    destinationId = generateRegionID(destination)

    if originId == None:
        print('No region id found for origin {}'.format(origin))
        return 
    if destinationId == None:
        print('No region id found for destination {}'.format(destination))
        return 

    packageType = "f+h"
    numberOfRooms = roomnumber
    adultsPerRoom = 2
    ftla = 'abc'
    ttla = 'def'

    for d in datelist:
        print('time: {} {} {}'.format(d[0].year,d[0].month,d[0].day))
        fromDate = str(d[0].year) + str("-") + str(d[0].month) + str("-") + str(d[0].day)
        toDate = str(d[1].year) + str("-") + str(d[1].month) + str("-") + str(d[1].day) 
        trips.append((packageType, origin, destination, fromDate, toDate, numberOfRooms, adultsPerRoom, originId, destinationId, ftla, ttla))

    return trips

# Generate URL based on trip information
def urlGenerateForPackage(trip):
    url = "https://www.expedia.com:443/getpackages/v1?packageType={packagetype}&origin={origin}&destination={destination}&fromDate={fromtime}&toDate={totime}&numberOfRooms={numroom}&adultsPerRoom%5B1%5D={adultperrm}&originId={originid}&destinationId={destinationid}&ftla={fromairport}&ttla={toairport}".format(packagetype=trip[0],origin=trip[1],destination=trip[2],fromtime=trip[3],totime=trip[4],numroom=trip[5],adultperrm=trip[6],originid=trip[7],destinationid=trip[8],fromairport=trip[9],toairport=trip[10])
    return url 

# Generate a list of URLs for popular city packages till the end of year
def createPopularCity(origins,destinations,defaultstartdays=7,forwarddays=7,intervaldays=7,tripdays=5,roomnumber=1):
    links = []
    for origin in origins:
        for destination in destinations:
            print('{} to {}'.format(origin,destination))
            if origin == destination: continue
            trip_details = generatePackageURLs(origin,destination,defaultstartdays,forwarddays,intervaldays,tripdays,roomnumber)
            if trip_details == None:
                print('cannot find trips for {} to {}'.format(origin,destination))
                continue
            for trip in trip_details:
                links.append(urlGenerateForPackage(trip))
    return links



def parsePackage(origins,destinations,defaultstartdays=7,forwarddays=7,intervaldays=7,tripdays=5,roomnumber=1):
    plist = createPopularCity(origins,destinations,defaultstartdays,forwarddays,intervaldays,tripdays,roomnumber)
    pac_dict = []
    frametime = str(datetime.datetime.now())
    for package in plist:
        try:
            request = requests.get(package)
            json_str = json.loads(request.content)
            # check if package has content
            if json_str['packageResult'] == None:
                print("No package content for {}".format(package))
                continue

            # package info           
            hotel_info = json_str['packageResult']['hotels']
            pac_info = json_str['packageResult']['packageOfferModels']
            adpac_info = json_str['packageResult'][
                'travelAdPackageOfferModels']
            flight_info = json_str['packageResult']['flights']

            # basic info
            startDate = json_str['packagePageInput']['startDate']['isoDate']
            endDate = json_str['packagePageInput']['endDate']['isoDate']
            origin = json_str['packagePageInput']['origin']
            destination = json_str['packagePageInput']['destination']
            timenow = str(datetime.datetime.now())
            # non-ad package details
            for p in pac_info:
                # id info
                purchaseURL = p['rateDetailsUrl']
                pacId = p['piid']
                hotelId = hotel_info[p['hotel']]['hotelId']
                flightId = p['flight']
                # price info
                price = p['price']
                # record price metric
                saveTotal = price['tripSavingsFormatted']
                savePerP = price['absSavingsPerPersonFormatted']
                pacTotal = price['packageTotalPriceFormatted']
                pacPerP = price['pricePerPersonFormatted']
                savedHotelPerN = price['hotelWithSavingsAppliedPricePerNightFormatted']
                fliAndHotTotal = price['sumFlightAndHotelFormatted']
                fliAndHotPerP = price['flightPlusHotelPricePerPersonFormatted']
                flightTotal = price['flightPriceFormatted']
                flightPerP = price['flightPricePerPersonFormatted']
                hotelTotal = price['hotelPriceFormatted']
                hotelPerN = price['hotelAvgPricePerNightFormatted']
                                                
                data_dict = {'pacId': pacId,
                             'startDate': startDate,
                             'endDate': endDate,                             
                             'hotelId': hotelId,
                             'flightId': flightId,                             
                             'origin': origin,
                             'destination': destination,
                             'timeChecked': timenow,
                             'fhPerPerson': fliAndHotPerP,
                             'fhTotal': fliAndHotTotal,
                             'hPerNight': hotelPerN,
                             'hTotal': hotelTotal,
                             'pricePerPerson': pacPerP,
                             'priceTotal': pacTotal,
                             'savePerPerson': savePerP,
                             'saveTotal': saveTotal,
                             'hPerNightWithSave': savedHotelPerN,
                             'url': package,
                             'purchaseURL':purchaseURL,
                             'flightInfo':flight_info
                             }
                pac_dict.append(data_dict)
            # clease sucessed package from Failed and Empty list
            print("load package successful {}".format(package))
        except:
            print("failed parse {}".format(package))
            continue
    return pac_dict


def multi_parsePackage_wrapper(args):
    return parsePackage(*args)

def createOriginframe(origin,defaultstartdays=7,forwarddays=7,intervaldays=7,tripdays=5,roomnumber=1):
    citylist = ['Denver','Orlando','Los Angeles','San francisco','Las Vegas','New Orleans','San Diego','Portland','New York','Washington DC']
    # citylist = ['Denver','Orlando']
    # Make the Pool of workers
    pool = Pool(20) 
    results = pool.map(multi_parsePackage_wrapper,[([origin],[c],defaultstartdays,forwarddays,intervaldays,tripdays,roomnumber) for c in citylist])
    #close the pool and wait for the work to finish 
    pool.close() 
    pool.join()  
    finalresults = [item for sublist in results for item in sublist]
    finalframe = dictToFrame(finalresults)
    finalframe.save(origin)
    # return
    return json.dumps(finalresults)

def multi_createOriginframe_wrapper(args):
    return createOriginframe(*args)

def backUpdate(defaultstartdays=7,forwarddays=7,intervaldays=7,tripdays=5,roomnumber=1):
    citylist = ['Denver','Orlando','Los Angeles','San francisco','Las Vegas','New Orleans','San Diego','Portland','New York','Washington DC']
    count = 1
    while True:
        print('iteration {} at {}'.foramt(count,datetime.datetime.now()))
        pool = Pool(20) 
        results = pool.map(multi_createOriginframe_wrapper,[([origin],defaultstartdays,forwarddays,intervaldays,tripdays,roomnumber) for origin in citylist])
        pool.close() 
        pool.join()
        time.sleep(600)
        count += 1


if __name__ == "__main__":
    # print(parsePackage(['New York'],['Denver']))
    # print(parsePackage(['New York'],['Orlando']))
    print(createOriginframe('New York'))




