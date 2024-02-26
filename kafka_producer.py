from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))

with open('ServerLog.log') as file:
    line_counter = 0
    for line in file:
        producer.send('log_topic', line.strip())
        line_counter += 1
        if line_counter % 1000 == 0:
            producer.flush()
            print(f'Sent {line_counter} lines to Kafka.')
    print(f'Sent {line_counter} lines to Kafka.')

print('Finished sending log data to Kafka.')




