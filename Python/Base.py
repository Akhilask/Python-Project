import datetime
import urllib.request
import zipfile   
import csv
import redis



def hello_redis():
    #redis connections
    redis_host = "localhost"
    redis_port = 6379
    redis_password = ""
    
    
    #path to store the downloaded zip file 
    file_name = "D:/PythonTest/test1zip.zip"
    
    #logic to build url based on current date
    curdate=datetime.datetime.now().strftime ("%d%m%y")
    url='https://www.bseindia.com/download/BhavCopy/Equity/'
    equrl="EQ"+ curdate
    print (equrl)
    newurl=url+equrl+"_CSV.ZIP" 
    print(newurl)
    
    #download a zip file
    urllib.request.urlretrieve(newurl,file_name)
    
    #open zip file
    zip = zipfile.ZipFile(file_name)
    
    #extract the zip file into a folder
    zip.extractall("D:\eclipse projects")      
    
    #read the csv file
    equity= equrl+".csv"
    folderurl=("D:/eclipse projects/"+equity)
    print (folderurl)
    with open(folderurl, mode='r') as csv_file:    
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            
            line_count += 1
            print(row)
            try:
                r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
                
                #set data into redis database
                r.hmset("d"+str(line_count), row) 
                #get data into redis database
                msg = r.hgetall("d")
                
                #create list
                new_list = []
                #insert data into lists
                new_list.append(row)  
                #print the data in list
                print(new_list)
                
                #exception block
            except Exception as e:
                print(e)
        
#main 
if __name__ == '__main__':
    hello_redis()