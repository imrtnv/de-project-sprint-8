from  psycopg2.extras import execute_values
import psycopg2

conn_settings = {
    "host": "localhost",
    "user": "jovyan",
    "password": "jovyan",
    "dbname": "de",
    "port": "5432"
}

conn = psycopg2.connect(**conn_settings)        
cur = conn.cursor()

sql = '''
CREATE TABLE public.subscribers_feedback (
  id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);
        '''

cur.execute(sql)
conn.commit()
cur.close()
conn.close()