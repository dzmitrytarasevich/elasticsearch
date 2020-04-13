import uuid
import time
import random
import datetime
from elasticsearch import Elasticsearch

types=['fixed', 'mobile']
sources=['customer','agent']
complaint=['yes','no','no']
is_manual=['yes','no','no']
is_authenticated=['yes','no']
sent_to_fullfil=['yes','no']
kpi1=30
kpi2=24*3600
kpi3=60

def generate_uuid():
    u=uuid.uuid4()
    return str(u)

def post_document(order_id,type,source,stage,received_time,status_time,expected_time,complains,in_process,sent_to_fulfill,authenticated,manual,status):
    es = Elasticsearch()
    doc = {
    'order_id':order_id,
    'type':type,
    'source':source,
    'stage':stage,
    'received_time':received_time,
    'status_time':status_time,
    'expected_time':expected_time,
    'complains':complains,
    'in_process':in_process,
    'sent_to_fulfill':sent_to_fulfill,
    'authenticated':authenticated,
    'manual':manual,
    'status':status,
    'datetime':datetime.datetime.now()
    }
    res = es.index(index="test-index", body=doc)


while True:
    #generate uuid for the order, choose a type and a source, log the time received
    # does the customer complain about the offer or not, set the expected time, and initate vars
    
    order_id=generate_uuid() 
    type=random.choice(types)
    source=random.choice(sources)
    #orders happened some time in the past (otherwise the kpi makes no sense)
    received_time=datetime.datetime.now()-datetime.timedelta(0,26*3600)+datetime.timedelta(0,random.randint(0,3600))
    complains=random.choice(complaint) #I guess we know this beforehand on first contact if he/she complains
    in_process='in process'
    sent_to_fulfill=''
    authenticated=''
    manual=''
    stage='order received'
    expected_time=''
    status_time=received_time
    status='ok'

    ###### Submit document here to elastic
    post_document(order_id,type,source,stage,received_time,status_time,expected_time,complains,in_process,sent_to_fulfill,authenticated,manual,status)
    ###############3
    #Here we are in the starting stage to capture the order
    #set the stage name, expected completion time, is the manual processing necessary and add some random ammount of time for    # the order to be completed
    stage='capture order'
    expected_time=received_time+datetime.timedelta(0,kpi1)
    
    #select a random processing time between 0 and 1.5 X kpi1 for the order (multiply by 3 if the order is manual
    time1=random.randint(0,2*kpi1)
    manual=random.choice(is_manual)
    
    if manual=='y':
       status_time=received_time+datetime.timedelta(0,4*time1)
    else:
       status_time=received_time+datetime.timedelta(0,time1)
    #for the results that make no sense (in the future), set the status_time to now 
    if status_time>datetime.datetime.now():
        status_time=datetime.datetime.now()
    #if the KPI is breached set status to breached and fail the order
    if status_time>expected_time:
        status='failed'
    ##### Submit document here to elastic
    post_document(order_id,type,source,stage,received_time,status_time,expected_time,complains,in_process,sent_to_fulfill,authenticated,manual,status)
    #start the next stage for the non-failed orders
    if status=='ok':
       #so we need to calculate how much time is allowed for the next stage
       stage='offer acceptance'
       expected_time=status_time+datetime.timedelta(0,kpi2)
       #choose if the authentication went successfull, if not, immediately discard the order
       authenticated=random.choice(is_authenticated)
       time2=random.randint(0,kpi2)
       if authenticated=='no':
           status='failed'
           status_time=datetime.datetime.now()
           post_document(order_id,type,source,stage,received_time,status_time,expected_time,complains,in_process,sent_to_fulfill,authenticated,manual,status)
           ### Document to elastic
       else:
           #how much time did it take to process the order, increase the status time by the random number
           status_time=status_time+datetime.timedelta(0,time2)
           if status_time>expected_time:
               status='failed'
               sent_to_fulfill='no'
               post_document(order_id,type,source,stage,received_time,status_time,expected_time,complains,in_process,sent_to_fulfill,authenticated,manual,status) 
               #### Document to elastic
           else:
               sent_to_fulfill='yes'
               post_document(order_id,type,source,stage,received_time,status_time,expected_time,complains,in_process,sent_to_fulfill,authenticated,manual,status) 
               #### Document to elastic
           if status=='ok':
               # time to begin the final stage, fullfilment,
               stage='fullfilment'
               expected_time=status_time+datetime.timedelta(0,kpi3)
               time3=random.randint(0,1.7*kpi3)
               status_time=status_time+datetime.timedelta(0,time3)
               if status_time>expected_time:
                   status='failed'
                   post_document(order_id,type,source,stage,received_time,status_time,expected_time,complains,in_process,sent_to_fulfill,authenticated,manual,status) 
                   #### Document to elastic
               else:
                   in_process='completed'
                   post_document(order_id,type,source,stage,received_time,status_time,expected_time,complains,in_process,sent_to_fulfill,authenticated,manual,status) 
                   #### Document to elastic
    
    #sleep a bit so we don't overflow the machine
    time.sleep(1) 
