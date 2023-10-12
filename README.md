# Stream corrector

This application connects to a WebSocket server that is supposed to send messages 
ordered by its sequence number stored in `id`, but it fails and sometimes sends
messages shuffled. The application organizes the received messages in order by sequence and forwards those messages to another WebSocket server.
The application reports in stdout the statistics on delays between received and sent 
messages as well as steps taken during the sorting for better debugging.

To test the application follow the steps:

- install libraries

```bash
pip install -r requirements.txt
```

- Visit [websocketking.com](https://websocketking.com/) and add two panels with 
WS addresses: `wss://test-ws.skns.dev/raw-messages` and `wss://test-ws.skns.dev/ordered-messages/Titov`. Press the `Connect` buttons on both panels. This tool will help you to see the result of the application.

- in 1st terminal run the simulation feeder to send data RawMessage server:

```bash
python feeder.py
```


- in 2nd terminal run the application:

```bash
python assignment.py
```

- check results on [websocketking.com](https://websocketking.com/)

- check file `latency_time.txt` for latency metrics in seconds.

## How it works

abstraction:

- get `new_id` value.
- if `new_id` == `prev_id+1` then send immediately, report latency time
- else receive data and store it in a buffer while `new_id` != `prev_id+1`
- if `new_id` == `fist_id_in_sorted_buffer` send, save `last_id` as `prev_id`, report latency time
- If the number of messages received is greater than 30 reset `prev_id` to `new_id`


