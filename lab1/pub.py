import redis
import time

# We use the connection pool to manage all connections to a redis server, 
# avoiding the overhead of establishing and releasing connections each time.
pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)

print("Connect successfully.")

id1 = "123321"
tag1 = {"Sport", "NBA", "James","Lebron"}
text1 = "LeBron James Crestfallen After Learning L.A. Doesn’t Have Any Rock And Roll Museums."

id2 = "133588"
tag2 = {"Politic", "The USA", "president"}
text2 = "Trump Claims He Rejected Trudeau's Meeting Request, No meeting was requested, Canadians say."

id3 = "143765"
tag3 = {"Business", "money", "company"}
text3 = "Ride-hailing firm Uber is paying $148m (£113m) to settle legal action over a cyber-attack that exposed data from 57 million customers and drivers."

# Creation of a database of the 3 news
r.hmset(id1,{"text" : text1, "tags" : tag1})
r.hmset(id2,{"text" : text2, "tags" : tag2})
r.hmset(id3,{"text" : text3, "tags" : tag3})

# The news will expire in 20 secs
r.expire(id1,20)
r.expire(id2,20)
r.expire(id3,20)

# Publish the news with their tags
time.sleep(3)
for t1 in tag1:
	r.publish(t1, id1)
print("Publish the news of id = " + id1)

time.sleep(2)
for t2 in tag2:
	r.publish(t2, id2)
print("Publish the news of id = " + id2)

time.sleep(2)
for t3 in tag3:
	r.publish(t3, id3)
print("Publish the news of id = " + id3)


