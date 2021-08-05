import boto3
# create a boto3 client
client = boto3.client('sqs')

queue_name = "crem-mel-sqs-poc"

# create the test queue
# for a FIFO queue, the name must end in .fifo, and you must pass FifoQueue = True
client.create_queue(QueueName=queue_name)
# get a list of queues, we get back a dict with 'QueueUrls' as a key with a list of queue URLs
queues = client.list_queues(QueueNamePrefix=queue_name) # we filter to narrow down the list
test_queue_url = queues['QueueUrls'][0]

print(test_queue_url)


# send 100 messages to this queue
for i in range(0,100):
    # we set a simple message body for each message
    # for FIFO queues, a 'MessageGroupId' is required, which is a 128 char alphanumeric string
    enqueue_response = client.send_message(QueueUrl=test_queue_url, MessageBody='This is test message #'+str(i))
    # the response contains MD5 of the body, a message Id, MD5 of message attributes, and a sequence number (for FIFO queues)
    print('Message ID : ',enqueue_response['MessageId'])

# next, we dequeue these messages - 10 messages at a time (SQS max limit) till the queue is exhausted.
# in production/real setup, I suggest using long polling as you get billed for each request, regardless of an empty response
while True:
    messages = client.receive_message(QueueUrl=test_queue_url,MaxNumberOfMessages=10) # adjust MaxNumberOfMessages if needed
    if 'Messages' in messages: # when the queue is exhausted, the response dict contains no 'Messages' key
        for message in messages['Messages']: # 'Messages' is a list
            # process the messages
            print(message['Body'])
            # next, we delete the message from the queue so no one else will process it again
            client.delete_message(QueueUrl=test_queue_url,ReceiptHandle=message['ReceiptHandle'])
    else:
        print('Queue is now empty')
        break

