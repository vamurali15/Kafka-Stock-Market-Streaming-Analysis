import pandas as pd
from kafka import KafkaProducer
from json import dumps
import json
from time import sleep


# In[13]:


producer = KafkaProducer(bootstrap_servers=['99.80.23.223:9092'],value_serializer = lambda x: dumps(x).encode('utf-8'))
# In[14]:


df = pd.read_csv("/home/ec2-user/indexProcessed.csv")


# In[ ]:


while True:
        sleep(1)
        producer.send('demo_testing2',value= df.sample(1).to_dict(orient = "records")[0])

