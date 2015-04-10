import json
import requests
from lxml import html
import re
import psycopg2

dbname = "yelp"
user = "postgres"
password = "data"

def getopenconnection():
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def create_table(tablename, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("CREATE TABLE "+str(tablename)+" (business_id int, user_id int, rating int);")
    conn.commit()

def delete_table(tablename, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("DROP TABLE "+str(tablename)+";")
    conn.commit()
    conn.close()

def insert(business_id,user_id,rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("INSERT INTO user_restaurant_ratings VALUES (%s, %s, %s)",(business_id,user_id,rating,))
    conn.commit()

def enter_users():
    conn = getopenconnection()

    create_table("users",conn)

    usersfile = open("/home/master/Downloads/yelp/yelpRestaurantUser.json").read()
    usersdata = json.loads(usersfile)
    index = 1
    for users in usersdata:

        if 'user_id' in users.keys():
            userid = str(users["user_id"])
            if 'name' in users.keys():
                name = users["name"]
            else:
                continue
        else:
            continue
        print (userid)
        print index
        insert(userid,name,index,conn)
        index += 1

    conn.close()

def enter_rest():
    conn = getopenconnection()

    create_table("restaurants",conn)

    restfile = open("/home/master/Downloads/yelp/yelpRestaurants.json").read()
    restdata = json.loads(restfile)

    index = 1
    for rest in restdata:

        business_id = str(rest["business_id"])
        city = str(rest["city"])
        review_count = int(rest["review_count"])
        name = rest["name"]
        openrest = str(rest["open"])
        address = str(rest["full_address"])
        state = str(rest["state"])
        stars = float(rest["stars"])
        lat = float(rest["latitude"])
        long = float(rest["longitude"])
        categories = str(rest["categories"])
        schools = str(rest["schools"])

        insert(business_id,index,city,review_count,name,openrest,address,state,stars,lat,long,categories,schools,conn)

        index += 1

    conn.close()

def enter_ratings():
    conn = getopenconnection()
    cur = conn.cursor()

    create_table("user_restaurant_ratings",conn)

    ratingsfile = open("/home/master/Downloads/yelp/yelpRestaurantUserReviews.json").read()
    ratingsdata = json.loads(ratingsfile)

    cur.execute("SELECT * FROM users;")
    users = cur.fetchall()

    cur.execute("SELECT * FROM restaurants;")
    restaurants = cur.fetchall()

    for ratings in ratingsdata:

        business_id = str(ratings["business_id"])
        user_id = str(ratings["user_id"])
        rating = int(ratings["stars"])

        for user in users:

            if user_id == user[0]:
                print user_id
                print user
                userindex = int(user[2])
                break

        for rest in restaurants:
            if business_id == rest[0]:
                print business_id
                print rest
                restindex = int(rest[1])
                break

        insert(restindex,userindex,rating,conn)

    conn.close()

if __name__ == '__main__':

    enter_ratings()