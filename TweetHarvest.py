from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from dbconn import *
import json
from twitter_credentials3 import API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
import sys

Pipeline = ItemPipeline()
index = int(sys.argv[1])

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):
    keywords = [['Your desired','key','words'],
            ['you can choose']]
    world = [-180,-90,180,90]
    loc   = [-58.5315, -34.7052, -58.3352, -34.5295]
    def on_data(self, data):
        #print '+1'
        tuit = json.loads(data)
        Pipeline.process_item(tuit, self.keywords[index])
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    #This handles Twitter authetification and the connection to Twitter Streaming API
    
    while True:
        l = StdOutListener()
        print l.keywords[index]
        auth = OAuthHandler(API_KEY, API_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        stream = Stream(auth, l)
        try:
            stream.filter(track= l.keywords[index],follow=None, locations= l.loc)
        except KeyboardInterrupt:
            print 'interrupted!'
            break
        except:
            print 'Hubo un problema'
            continue