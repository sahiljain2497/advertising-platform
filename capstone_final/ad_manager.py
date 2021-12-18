import sys
import mysql.connector
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable
import json
from datetime import datetime

class KafkaMySQLSink:
    def __init__(
        self,
        kafka_bootstrap_server,
        kafka_topic_name,
        database_host,
        database_username,
        database_password,
        database_name,
    ):
        #Initialize Kafka Consumer
        kafka_client = KafkaClient(kafka_bootstrap_server)
        self.consumer = kafka_client  \
            .topics[kafka_topic_name]  \
            .get_simple_consumer(consumer_group="groupid", auto_offset_reset=OffsetType.LATEST)
        # Initialize MySQL database connection
        self.db = mysql.connector.connect(
            host=database_host,
            user=database_username,
            password=database_password,
            database=database_name,
            charset="utf8mb4",
            use_unicode=True
        )

    def calc_date_time_diff_in_minutes(self,msg):
        start_date = msg.get('date_range').get('start').split('-')
        end_date = msg.get('date_range').get('end').split('-')
        start_time = msg.get('time_range').get('start').split(':')
        end_time = msg.get('time_range').get('end').split(':')
        datetime_start = datetime(int(start_date[0]),int(start_date[1]),int(start_date[2]),int(start_time[0]),int(start_time[1]),int(start_time[2]))
        datetime_end = datetime(int(end_date[0]),int(end_date[1]),int(end_date[2]),int(end_time[0]),int(end_time[1]),int(end_time[2]))
        datetime_diff = datetime_end - datetime_start
        datetime_diff_minutes = datetime_diff.total_seconds() / 60
        return datetime_diff_minutes

    # Process single row
    def process_row(self, msg):
        # Get the db cursor
        db_cursor = self.db.cursor()
        
        #calculate CPM by provided formula
        cpm = (0.0075 * float(msg.get('cpc'))) + (0.0005 * float(msg.get('cpa')))
        
        #calculate current slot budget as per 10 minute interval
        datetime_diff_minutes = self.calc_date_time_diff_in_minutes(msg)
        slot_time_size = 10
        number_of_slots = datetime_diff_minutes/slot_time_size
        current_slot_budget = msg.get('budget')/number_of_slots
        
        #status of campaign
        status = 'INACTIVE 'if msg.get('action') == 'Stop Campaign' else 'ACTIVE'
        
        # DB query for supporting UPSERT operation
        sql = "REPLACE INTO ads(text,category,keywords,campaign_id,status,target_gender,target_age_start,target_age_end,target_city,target_state \
            ,target_country,target_income_bucket,target_device,cpc,cpa,cpm,budget,current_slot_budget,date_range_start,date_range_end,time_range_start,time_range_end) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s)" 
        #values from kafka msg
        val = (msg.get('text'),msg.get('category'), msg.get('keywords'), msg.get('campaign_id'),status, msg.get('target_gender'), msg.get('target_age_range').get('start'), msg.get('target_age_range').get('end')\
            , msg.get('target_city'), msg.get('target_state'), msg.get('target_country'), msg.get('target_income_bucket'), msg.get('target_device'), msg.get('cpc'), msg.get('cpa'), cpm, msg.get('budget')\
            , current_slot_budget, msg.get('date_range').get('start'), msg.get('date_range').get('end'), msg.get('time_range').get('start'), msg.get('time_range').get('end') )
        db_cursor.execute(sql, val)
        # Commit the operation, so that it reflects globally
        self.db.commit()
        #output to console
        print(msg.get('campaign_id') + ' | ' + msg.get('action') + ' | ' + status)

    # Process kafka queue messages
    def process_events(self):
        try:
            for queue_message in self.consumer:
                if queue_message is not None:
                    msg = queue_message.value
                    msg = json.loads(msg)
                    self.process_row(msg)
                    # In case Kafka connection errors, restart consumer ans start processing
        except (SocketDisconnectedError, LeaderNotAvailable) as e:
            self.consumer.stop()
            self.consumer.start()
            self.process_events()

    def __del__(self):
        # Cleanup consumer and database connection before termination
        self.consumer.stop()
        self.db.close()


if __name__ == "__main__":
    # Validate Command line arguments
    if len(sys.argv) != 7:
        print(
            "Usage: kafka_mysql.py <kafka_bootstrap_server> <kafka_topic> <database_host> <database_username> <database_password> <database_name>"
        )
        exit(-1)
    kafka_bootstrap_server = sys.argv[1]
    kafka_topic = sys.argv[2]
    database_host = sys.argv[3]
    database_username = sys.argv[4]
    database_password = sys.argv[5]
    database_name = sys.argv[6]
    kafka_mysql_sink = None
    try:
        kafka_mysql_sink = KafkaMySQLSink(kafka_bootstrap_server, kafka_topic, database_host, database_username, database_password, database_name)
        kafka_mysql_sink.process_events()
    except KeyboardInterrupt:
        print("KeyboardInterrupt, exiting...")
    finally:
        if kafka_mysql_sink is not None:
            del kafka_mysql_sink