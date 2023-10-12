import asyncio
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
import json
import random
import settings

async def send_messages(uri):
    id_seq = 0  # Initialize outside the loop to keep it persistent
    while True:  # Outer loop to reconnect
        try:
            async with websockets.connect(uri) as websocket:
                print("Connection established. Sending messages...")
                pending_messages = []

                while True:  # Inner loop to keep sending messages
                    id_seq += 1  # Incrementing continues from the last value
                    random_text = random.randint(1, 100000)
                    message = json.dumps({"id": id_seq, "text": random_text})
                    pending_messages.append(message)

                    if len(pending_messages) >= 3:
                        if random.random() < 0.5:
                            random.shuffle(pending_messages)

                        msg_to_send = pending_messages.pop(0)
                        await websocket.send(msg_to_send)
                        print(f"Sent message: {msg_to_send}")

                    sleep_time = random.uniform(0.1, 1)
                    # sleep_time=0.01
                    await asyncio.sleep(sleep_time)

        except (ConnectionClosedError, ConnectionClosedOK):
            print("Connection lost. Reconnecting in 3 seconds...")
            await asyncio.sleep(3)  # Wait for 3 seconds before reconnecting

if __name__ == "__main__":
    uri = settings.consumer_uri
    asyncio.run(send_messages(uri))
