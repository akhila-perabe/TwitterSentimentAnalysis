import tweepy
import socket
import sys
import re
import string
import json
import requests

ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

TCP_IP = 'localhost'
TCP_PORT = 9001

def cleanupTwitterText(line):
    if line==None:
        return line

    line = re.sub(r'[.,"!#]+', '', line, flags=re.MULTILINE)  # removes the characters specified
    line = re.sub(r'^RT[\s]+', '', line, flags=re.MULTILINE)  # removes RT
    line = re.sub(r'https?:\/\/.*[\r\n]*', '', line, flags=re.MULTILINE)  # remove link
    line = re.sub(r'[:]+', '', line, flags=re.MULTILINE)
    line = ''.join(list(filter(lambda x: x in string.printable, line)))  # filter non-ascii characers

    new_line = ''
    for i in line.split():  # remove @ and #words, punctuataion
        if not i.startswith('@') and i not in string.punctuation:
            new_line += i + ' '
    return new_line

#Listener class to read the stream
class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        msg = cleanupTwitterText(status.text)
        location = cleanupTwitterText(status.user.location)
		
        #print ("Tweet: ", status.created_at, location, msg)
        
        dict = None
        if location != None:
            #Get the GeoLocation using Google APIs
            parameters = {"address" : '+'.join(location.split(' ')), "key": "myKey"}
            response = requests.get("https://maps.googleapis.com/maps/api/geocode/json", params=parameters)
            if response.status_code==200:
                response = json.loads(response.content.decode("utf-8"))
                if response["status"] == "OK":
                    results = response["results"]
                    dict = results[0]["geometry"]["location"]
                
        if dict != None:
            data = json.dumps({"status":msg, "location":dict, "timestamp": str(status.created_at)}) + "\n"
            conn.send(data.encode('utf-8'))
    
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)
			

# Authenticate tweepy to access tweets
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

# Get the hash tag from command line
if len(sys.argv) < 2:
	print ("Invalid command: stream.py <HashtagString>")
	sys.exit(0)
hashtag = sys.argv[1:]

# create server socket and wait for client to connect
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
print ("Waiting for incoming connection")
s.listen(1)
conn, addr = s.accept()

#Create the tweepy stream to read twitter data
myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())

#Filter the required hastag
myStream.filter(track=hashtag)

#Close the socket
s.close()
conn.close()


