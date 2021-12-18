import sys
import mysql.connector
import json
from datetime import datetime

#custom class for creating dictionary
class dotdict(dict):
  __getattr__ = dict.get
  __setattr__ = dict.__setitem__
  __delattr__ = dict.__delitem__

def calc_date_time_diff_in_minutes(msg):
    start_date = msg['date_range_start'].split('-')
    end_date = msg['date_range_end'].split('-')
    start_time = msg['time_range_start'].split(':')
    end_time = msg['time_range_end'].split(':')
    datetime_start = datetime(int(start_date[0]),int(start_date[1]),int(start_date[2]),int(start_time[0]),int(start_time[1]),int(start_time[2]))
    datetime_end = datetime(int(end_date[0]),int(end_date[1]),int(end_date[2]),int(end_time[0]),int(end_time[1]),int(end_time[2]))
    datetime_diff = datetime_end - datetime_start
    datetime_diff_minutes = datetime_diff.total_seconds() / 60
    return datetime_diff_minutes

#get resuls from db with headers
def get_results(db_cursor):
    desc = [d[0] for d in db_cursor.description]
    results = [dotdict(dict(zip(desc, res))) for res in db_cursor.fetchall()]
    return results

#function to redistribute budget, logic same as process row in ad_manager
def redistribute_budget():
  db_cursor = db.cursor()
  db_cursor.execute("select * from advertisement.ads")
  rows = get_results(db_cursor)
  db.commit()
  for msg in rows:
    db_cursor = db.cursor()
    #calculate CPM by provided formula
    cpm = (0.0075 * float(msg['cpc'])) + (0.0005 * float(msg['cpa']))
    #calculate current slot budget as per 10 minute interval
    datetime_diff_minutes = calc_date_time_diff_in_minutes(msg)
    slot_time_size = 10
    number_of_slots = datetime_diff_minutes/slot_time_size
    current_slot_budget = msg['budget']/number_of_slots
    
    #status of campaign
    status = 'INACTIVE 'if msg['budget'] <= 0 else 'ACTIVE'
    sql = 'update ads set current_slot_budget='+str(current_slot_budget)+',status="'+ str(status)+'" where campaign_id="'+ str(msg['campaign_id']) + '"'
    print(sql)
    # DB query for supporting UPSERT operation
    db_cursor.execute(sql)
    # Commit the operation, so that it reflects globally
    db.commit()

if __name__ == "__main__":
  # Validate Command line arguments
  if len(sys.argv) != 5:
    print(
        "Usage: slot_budget_manager.py <database_host> <database_username> <database_password> <database_name>"
    )
    exit(-1)
  database_host = sys.argv[1]
  database_username = sys.argv[2]
  database_password = sys.argv[3]
  database_name = sys.argv[4]

  #connect to mysql server
  db = mysql.connector.connect(
    host=database_host,
    user=database_username,
    password=database_password,
    database=database_name,
    charset="utf8mb4",
    use_unicode=True
  )

  redistribute_budget()