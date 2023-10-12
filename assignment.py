import asyncio
import websockets
import settings
import json
import time

# solution:
# get last_id value.
# if new_id == prev_id+1 then send immediately, save latency time
# else receive data while new_id != prev_id+1 
# if new_id == prev_id+1 sort and send save last_id as prev_id, save latency time
# if qty of message receved id greater than 10 sort and send save last_id as prev_id, save latency time

def save_latency_time(max_latency_time, min_latency_time):
    with open("latency_time.txt", "w") as f:
        f.write("Max latency time: " + str(max_latency_time) + "\n")
        f.write("Min latency time: " + str(min_latency_time) + "\n")

async def consumer_handler(consumer_ws, producer_ws):
    prev_id=0
    last_id=0
    count_messages=0
    unsorted_buffer=[]
    expected_id=0
    max_latency_time=0
    min_latency_time=3600

    # unsorted_buffer.append(message_dict)
    async for message in consumer_ws:
        start_time = time.time()
        count_messages += 1 
        print(f"{count_messages}---------------------")
        message_dict=json.loads(message)
        # unsorted_buffer.append(message_dict)
        buffer_size=len(unsorted_buffer)
        new_id=message_dict['id']
        if(count_messages==1):
            expected_id=new_id
        if(abs(new_id-expected_id)>30):
            expected_id=new_id
            unsorted_buffer=[]
            print(f"Data is scrambled,  expected {expected_id}")

        print(f"Received {new_id} expected {expected_id}")
        if(new_id==expected_id):
            await producer_ws.send(f"{message_dict}")
            end_time = time.time()
            elapsed_time = end_time - start_time
            if(elapsed_time>max_latency_time):
                max_latency_time=elapsed_time
                save_latency_time(max_latency_time, min_latency_time)
            if(elapsed_time<min_latency_time):
                min_latency_time=elapsed_time
                save_latency_time(max_latency_time, min_latency_time)

            print(f"Latency: {elapsed_time} sec.")
            print(f"Send {new_id}")
            expected_id=new_id+1
            print(f"Expecting {expected_id}")
            if(len(unsorted_buffer)>0):
                sorted_list = sorted(unsorted_buffer, key=lambda x: x['id'])
                if(sorted_list[0]['id']==expected_id):
                    buffer_id=0
                    for item in sorted_list:
                        print(f"Send_ {item['id']}")
                        await producer_ws.send(f"{item}")
                        buffer_id=item['id']
                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    if(elapsed_time>max_latency_time):
                        max_latency_time=elapsed_time
                        save_latency_time(max_latency_time, min_latency_time)
                    if(elapsed_time<min_latency_time):
                        min_latency_time=elapsed_time
                        save_latency_time(max_latency_time, min_latency_time)
                    print(f"Latency: {elapsed_time} sec.")
                    expected_id=buffer_id+1
                    unsorted_buffer=[]
                    print(f"Expecting {expected_id}  b:({len(unsorted_buffer)})")
                else:
                    print(f"{sorted_list[0]['id']} != {expected_id}")
        else:
            if(len(unsorted_buffer)==0):
                print(f"Buffer empty Expecting {expected_id}")
            if(new_id>expected_id):
                unsorted_buffer.append(message_dict)
                print(f"collect {new_id} b:({len(unsorted_buffer)})")
            else:
                print(f"outdated {new_id}!")


async def main():
    consumer_uri = settings.consumer_uri 
    producer_uri = settings.producer_uri

    while True:
        try:
            async with websockets.connect(consumer_uri, ping_timeout=None, ping_interval=5) as consumer_ws:
                print(f"Connected to consumer WebSocket {consumer_uri}.")

                async with websockets.connect(producer_uri, ping_timeout=None, ping_interval=5) as producer_ws:
                    print("Connected to producer WebSocket.")
                    print("Listening to the stream...")
                    
                    await consumer_handler(consumer_ws, producer_ws)
        except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK):
            print("Connection closed, reconnecting...")
            await asyncio.sleep(2)  # Wait for 3 seconds before reconnecting


if __name__ == "__main__":
    asyncio.run(main())