import sys
import flask
from flask import request, jsonify
import uuid
from flask import abort
import mysql.connector
from ast import literal_eval
from time import time
from datetime import datetime

#create flask app with debug true
app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config['RESTFUL_JSON'] = {"ensure_ascii": False}

#fetch user
def get_user_data(user_id):
	db_cursor = db.cursor()
	db_cursor.execute("SELECT * FROM users where id = '" + user_id + "'")
	rows = get_results(db_cursor)
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

#save ad served to user in database;
def create_served_ad_entry(request_id, ad, user, auction):
	db_cursor = db.cursor()
	user_id = user['id'] if user else None;
	campaign_start_time = ad['date_range_start'] + ' ' + ad['time_range_start']
	campaign_end_time = ad['date_range_end'] + ' ' + ad['time_range_end']
	target_location =  ad['target_city'] + ', '  + ad['target_state'] + ', '  + ad['target_country']
	target_age_range = str(ad['target_age_start']) + '-' + str( ad['target_age_end'])
	ts = time()
	timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
	sql = 'insert into served_ads(request_id,campaign_id,user_id,auction_cpm,auction_cpc,auction_cpa, \
		target_age_range,target_location,target_gender,target_income_bucket,target_device_type,campaign_start_time, \
		campaign_end_time,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
	val = (request_id, ad['campaign_id'], user_id, auction['cpm'], auction['cpc'], auction['cpa'], target_age_range , target_location, ad['target_gender'], \
		ad['target_income_bucket'], ad['target_device'], campaign_start_time, campaign_end_time, timestamp)
	db_cursor.execute(sql,val)
	db.commit()

#get ad campaign to be run
def target_ad_campaign(device_type, city, state, user):
	db_cursor = db.cursor()
	device_type = ' '.join(device_type.split('-'))
	#get eligible ad
	sql = "select * from advertisement.ads  where status = 'ACTIVE' and current_slot_budget > 0 and (target_device = 'All' or target_device like '" + device_type + "') and \
		(target_city = 'All' or target_city like '" + city + "') and (target_state = 'All' or target_state like '" + state + "')"
	#user age check
	if user['age'] : 
		sql  = sql + " and (target_age_start <= " + str(user['age']) + " and " + str(user['age']) +" <= target_age_end)" 		
	#user gender check
	if user['gender'] : 
		sql  = sql + " and (target_gender = 'All' or target_gender like '" + str(user['gender'])  + "')" 		
	#user income bucket check
	if user['income_bucket'] : 
		sql  = sql + " and (target_income_bucket = 'All' or target_income_bucket like '" + str(user['income_bucket'])  + "')" 		
	sql = sql + " order by cpm desc limit 2"
	db_cursor.execute(sql)
	rows = get_results(db_cursor)
	#handle 0 rows returned here?
	if len(rows) == 0:
		#no eligble ads;
		return None
	if len(rows) == 1:
		#If there is a single eligible Ad, the campaigner has to pay the price same as the original bid.
		ad = rows[0]
		return { 'ad' : ad, 'auction' : { "cpm" : ad['cpm'], 'cpc' : ad['cpc'], 'cpa' : ad['cpa']} }
	else: 
		#The highest bidder will win the auction, but the winner has to pay the price that is bid by the second-highest bidder.
		ad_first = rows[0]
		ad_second = rows[1]
		return { 'ad' : ad_first, 'auction' : { "cpm" : ad_second['cpm'], 'cpc' : ad_second['cpc'], 'cpa' : ad_second['cpa']} }

#get ad campaign to be run without user info
def non_targeted_ad_campaign(device_type, city, state):
	db_cursor = db.cursor()
	device_type = ' '.join(device_type.split('-'))
	db_cursor.execute("select * from advertisement.ads  where status = 'ACTIVE' and current_slot_budget > 0 and (target_device = 'All' or target_device like '" + device_type + "') and \
		(target_city = 'All' or target_city like '" + city + "') and (target_state = 'All' or target_state like '" + state + "') \
		order by cpm desc limit 2")
	rows = get_results(db_cursor)
	if len(rows) == 0:
		#no eligble ads;
		return None
	if len(rows) == 1:
		#If there is a single eligible Ad, the campaigner has to pay the price same as the original bid.
		ad = rows[0]
		return { 'ad' : ad, 'auction' : { "cpm" : ad['cpm'], 'cpc' : ad['cpc'], 'cpa' : ad['cpa']} }
	else: 
		#The highest bidder will win the auction, but the winner has to pay the price that is bid by the second-highest bidder.
		ad_first = rows[0]
		ad_second = rows[1]
		return { 'ad' : ad_first, 'auction' : { "cpm" : ad_second['cpm'], 'cpc' : ad_second['cpc'], 'cpa' : ad_second['cpa']} }

#test route
@app.route('/ping', methods=['GET'])
def ping():
	return "pong"

#add serving route
@app.route('/ad/user/<user_id>/serve', methods=['GET'])
def serve(user_id):
	query_params = request.args
	# Parameter validation
	if "device_type" not in query_params or "state" not in query_params or "city" not in query_params:
		return abort(400)
	device_type = query_params['device_type']
	state = query_params['state']
	city = query_params['city']
	user = None
	res = None
	# Generate the request identifier
	request_id = str(uuid.uuid1())
	if user_id != '1111-1111-1111-1111': 
		user = get_user_data(user_id)
		if not user:
			return abort(400)
		res = target_ad_campaign(device_type, city, state, user)
	else:
		res = non_targeted_ad_campaign(device_type, city, state)
	if not res: 
		return abort(404)
	create_served_ad_entry(request_id, res['ad'], user, res['auction'])
	return jsonify({
		"text": res['ad']['text'],
		"request_id": request_id
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
	app.run(host="localhost", port=5000)

