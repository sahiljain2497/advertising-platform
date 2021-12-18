import sys
import flask
from flask import request, jsonify
import uuid
from flask import abort
import mysql.connector
from ast import literal_eval
from time import time
from datetime import datetime
from time import sleep
from json import dumps
from kafka import KafkaProducer

#create flask app with debug true
app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config['RESTFUL_JSON'] = {"ensure_ascii": False}

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x,sort_keys=True, default=str).encode('utf-8'))
#custom class for creating dictionary
class dotdict(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

#get resuls from db with headers
def get_results(db_cursor):
    desc = [d[0] for d in db_cursor.description]
    results = [dotdict(dict(zip(desc, res))) for res in db_cursor.fetchall()]
    return results

#fetch user
def get_user_data(user_id):
	db_cursor = db.cursor()
	db_cursor.execute("SELECT * FROM users where id = '" + user_id + "'")
	rows = get_results(db_cursor)
	db.commit()
	if len(rows) == 0 : 
		return None
	user = rows[0]
	user['programs'] = literal_eval(user['programs'])
	user['music'] = literal_eval(user['music'])
	user['movies'] = literal_eval(user['movies'])
	user['websites'] = literal_eval(user['websites'])
	user['negatives'] = literal_eval(user['negatives'])
	user['positives'] = literal_eval(user['positives'])
	return user

#get served ad from db
def get_served_ad(ad_request_id):
  db_cursor = db.cursor()
  db_cursor.execute("SELECT * FROM served_ads where request_id = '" + ad_request_id + "'")
  rows = get_results(db_cursor)
  db.commit()
  if len(rows) == 0 :
    return None
  served_ad = rows[0]
  return served_ad

#get served ad campaign from db
def get_ad_campaign(campaign_id):
  db_cursor = db.cursor()
  db_cursor.execute("SELECT * FROM ads where campaign_id = '" + campaign_id + "'")
  rows = get_results(db_cursor)
  db.commit()
  if len(rows) == 0 :
    return None
  campaign = rows[0]
  return campaign

#update served ad campaign in db
def update_campaign(campaign_id, budget, current_slot_budget):
  db_cursor = db.cursor()
  status = "INACTIVE" if budget <= 0 else "ACTIVE"
  sql = 'update ads set budget=' + str(budget) + ',current_slot_budget='+str(current_slot_budget)+',status="'+ str(status)+'" where campaign_id="'+ str(campaign_id) + '"'
  db_cursor.execute(sql)
  db.commit()

#writing down feedback to internal kafka
def send_feedback_to_kafka(served_ad, user_action, expenditure, view, click, acquisition):
  #form dict for feedback object
  d = {
    'campaign_id': served_ad['campaign_id'],
    'user_id' : served_ad['user_id'],
    'request_id' : served_ad['request_id'],
    'click' : click,
    'view' : view,
    'acquisition' : acquisition,
    'auction_cpm' : served_ad['auction_cpm'],
    'auction_cpc' : served_ad['auction_cpc'],
    'auction_cpa' : served_ad['auction_cpa'],
    'target_age_range' : served_ad['target_age_range'],
    'target_location' : served_ad['target_location'],
    'target_gender' : served_ad['target_gender'],
    'target_income_bucket' : served_ad['target_income_bucket'],
    'target_device_type	' : served_ad['target_device_type'],
    'campaign_start_time' : served_ad['campaign_start_time'],
    'campaign_end_time' : served_ad['campaign_end_time'],
    'user_action' : user_action,
    'expenditure' : expenditure,
    'timestamp' : served_ad['timestamp']
  }

  # temp insert into mysql for testing my reports from hue
  db_cursor = db.cursor()
  sql = 'insert into user_feedback(campaign_id,user_id,request_id,click,view,acquisition,auction_cpm,auction_cpc,auction_cpa,target_age_range,target_location,target_gender,target_income_bucket,target_device_type,campaign_start_time,campaign_end_time,user_action,expenditure,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
  val = tuple(d.values())
  print(val)
  db_cursor.execute(sql,val)
  db.commit()
  #print('sending feedback to kafka')
  #print(d)
  #publish data to kafka
  producer.send('user-feedback', d)

#test route
@app.route('/ping', methods=['GET'])
def ping():
	return "pong"

#add serving route
@app.route('/ad/<ad_request_id>/feedback', methods=['POST'])
def feedback(ad_request_id):
  if not request.json:
    abort(400)
  view = request.json['view']
  click = request.json['click']
  acquisition = request.json['acquisition']
  print(view,click,acquisition)
  #find served ad in db
  served_ad = get_served_ad(ad_request_id)
  if not served_ad:
    print('served ad not found')
    abort(400)
  #calculate expenditure and what was the user action
  expenditure = 0
  user_action = 'view'
  if acquisition == 1:
    user_action = 'acquisition'
    expenditure = served_ad['auction_cpa']
  if click == 1 and acquisition == 0:
    user_action = 'click'
    expenditure = served_ad['auction_cpc']
  #find served_ad campaign so we can check budget etc
  campaign = get_ad_campaign(served_ad['campaign_id'])
  if not campaign:
    print('campaign not found')
    abort(400)
  #print(campaign)
  #calculate campaign slot budget and budget
  new_current_slot_budget  = campaign['current_slot_budget'] - expenditure
  new_budget = campaign['budget'] - expenditure
  #update campaign slot budget and budget
  update_campaign(campaign['campaign_id'], new_budget, new_current_slot_budget)
  send_feedback_to_kafka(served_ad, user_action, expenditure, view, click, acquisition)
  return jsonify({
    "status" : "SUCCESS"
	})

if __name__ == "__main__":
	database_host = sys.argv[1]
	database_username = sys.argv[2]
	database_password = sys.argv[3]
	database_name = sys.argv[4]
    # Start Flask application
    #connect to mysql server
	db = mysql.connector.connect(
			host=database_host,
			user=database_username,
			password=database_password,
			database=database_name,
			charset="utf8mb4",
			use_unicode=True
    )
	app.run(host="localhost", port=5001)

