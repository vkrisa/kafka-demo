from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition
import json

client = 'localhost:9092' # Server address and port
input_topic = 'input' # Name of the input topic in kafka
output_topic = 'output' # Name of the output topic in kafka

# Consumer class for reading kafka messages from a specific topic.
class Consumer():
    def __init__(self, address, topic):
        self.address = address
        self.topic = topic
        self.tp = TopicPartition(self.topic, 0)
        self.consumer = KafkaConsumer(bootstrap_servers=address)
        self.consumer.assign([self.tp])
        self.consumer.seek_to_end(self.tp)
        self.last = self.consumer.position(self.tp) # Get the last message offset, to avoid infinite loop.
        self.consumer.seek_to_beginning(self.tp) # Back to the first message.
        self.messages = [] # Init a list for the messages coming from kafka.
    
    # Transform the line of strings to a list of strings, for the easier process.
    def consumeMessages(self):
        for msg in self.consumer:
            line = msg.value.decode("utf-8")
            splitted_message = line.split(',')
            self.messages.append(splitted_message)
            if msg.offset == self.last - 1:
                break
    
    # Filter the ages less than the choosen limit.
    def ageFilter(self, limit, age_index=3):
        self.messages = [x for x in self.messages if int(x[age_index]) > int(limit)]
    
    # Filter list of specific countrys.
    def countryFilter(self, value, loc_index=2):
        self.messages = [x for x in self.messages if x[loc_index] in value]
    
    def getMessages(self):
        return self.messages   
    
    # Drop the country column
    def dropCountry(self, loc_index=2):
        for i in self.messages:
            i.pop(loc_index)
       
    
class Producer():
    def __init__(self,address,data):
        self.data = data
        self.producer = KafkaProducer(bootstrap_servers=address)
    
    # Dump the strings to a json and byte stream format.
    def sendMessages(self, topic):
        for element in self.data:
            jd = json.dumps(element)
            self.producer.send(topic, jd.encode('utf-8'))
            
    def getMessages(self):
        return self.data
       
        
if __name__ == '__main__':
    consumer = Consumer(client, input_topic)
    consumer.consumeMessages()
    consumer.ageFilter(18)
    consumer.countryFilter(['HU', 'US'])
    consumer.dropCountry()
    data = consumer.getMessages()

    producer = Producer(client,data)
    producer.sendMessages(output_topic)

    newconsumer = Consumer(client,output_topic)
    newconsumer.consumeMessages()
    print(newconsumer.getMessages())
