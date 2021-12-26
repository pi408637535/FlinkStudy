from kafka import KafkaProducer
import json
import numpy as np
import time
import datetime

bootstrap_server = "10.103.17.101:9092,10.103.17.102:9092,10.103.17.103:9092,10.103.17.104:9092"
# producer = KafkaProducer(bootstrap_servers=bootstrap_server)   #连接kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_server,value_serializer=lambda m: json.dumps(m).encode('utf-8'))
msg = "Hello, kafka1111"
old_msg = None
for i in range(100):

    if old_msg != None:
        t = datetime.datetime.now()
        t2 = (t - datetime.timedelta(microseconds=30)).strftime("%Y-%m-%d %H:%M:%S.%f")
        ts2 = time.mktime(time.strptime(t2, '%Y-%m-%d %H:%M:%S.%f'))
        old_msg["timestamp"] = int(ts2)
        old_msg["temperature"] = old_msg["temperature"] - 1

        producer.send('cpp_trace_test_pi', old_msg)
        print(old_msg)
        print("++++++++++")

    id = np.random.randint(0,4,1)[0]
    name = "sensor_{0}".format(id)
    timestamp = int(time.time())
    temperature = 30 + round(np.random.random(1)[0] * 10, 1)
    msg = {"name":name, "timestamp":timestamp, "temperature":temperature}
    # msg


    producer.send('cpp_trace_test_pi', msg)

    print(msg)
    time.sleep(0.5)

    # producer.send('cpp_trace_test_pi', msg)
    # print(msg)
    old_msg = msg
    print("--------")
    # time.sleep(0.5)


producer.close()
