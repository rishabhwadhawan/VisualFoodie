import psycopg2

dbname = "yelp"
user = "postgres"
password = "data"

def getopenconnection():
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def recommendation(user_id,openconnection):
    conn = openconnection

    cur = conn.cursor()

    cur.execute("SELECT index FROM users WHERE id = "+"\""+str(user_id)+"\";")
    index = cur.fetchall()

    cur.execute("SELECT * FROM user_restaurant_ratings R RECOMMEND R.business_id TO R.user_id ON R.stars USING ItemCosCF WHERE R.user_id = "+ str(index)+" ORDER BY R.stars DESC LIMIT 20;")

    result = cur.fetchall()

    print result
if __name__ == '__main__':
    conn = getopenconnection()
    #recommendation("value of userid, conn)
    conn.close()