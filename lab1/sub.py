import redis
import time

# We use the connection pool to manage all connections to a redis server, 
# avoiding the overhead of establishing and releasing connections each time.
pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)

client1 = r.pubsub()
client2 = r.pubsub()

print("Client1 connect the channel successfully.")
print("Client2 connect the channel successfully.")
time.sleep(1)

tag1 = "Sport"
tag2 = "Politic"
tag3 = "Business"

# Subscribe
client1.subscribe(tag1)
client1.subscribe(tag2)
client2.subscribe(tag1)
client2.subscribe(tag3)

print("Client1 subscribes the news about 《" + tag1 + "》")
print("Client1 subscribes the news about 《" + tag2 + "》")
print("Client2 subscribes the news about 《" + tag1 + "》")
print("Client2 subscribes the news about 《" + tag3 + "》")

time.sleep(1)

# Unsubscribe
client1.unsubscribe(tag2)
print("Client1 unsubscribes the news about 《" + tag2 + "》")

# Listen to the channel and get the information of publish
while True:
    message1 = client1.get_message()
    message2 = client2.get_message()
    if message1 and message1['type'] == 'message':
        news1 = r.hget(message1['data'],"text")
        print ("Client1 receives a news about 《" +message1['channel']+ "》:\n" + news1)
        time.sleep(1)
    if message2 and message2['type'] == 'message':
        news2 = r.hget(message2['data'],"text")
        print ("Client2 receives a news about 《" +message2['channel']+ "》:\n" + news2)


