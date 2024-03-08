import socket
import threading

# Initialize the data_store for storing key-value pairs
data_store = {}

def addDataStream(stream_key, entry_id, *key_value_pairs):
    if stream_key not in data_store:
        data_store[stream_key] = {"type": "stream", "value": []}
    last_entry_id = data_store[stream_key]["value"][-1]["id"] if data_store[stream_key]["value"] else "0-0"
    last_ms, last_seq = map(int, last_entry_id.split("-"))
    current_ms, current_seq = entry_id.split("-")
    current_ms = int(current_ms)
    if current_seq == "*":
        if current_ms > last_ms:
            current_seq = 0
        else:
            current_seq = last_seq + 1
        entry_id = f"{current_ms}-{current_seq}"
    else:
        current_seq = int(current_seq)
    if current_ms == 0 and current_seq == 0:
        return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
    if current_ms < last_ms or (current_ms == last_ms and current_seq <= last_seq):
        return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
    entry = {"id": entry_id}
    for i in range(0, len(key_value_pairs), 2):
        key = key_value_pairs[i]
        value = key_value_pairs[i+1]
        entry[key] = value
    data_store[stream_key]["value"].append(entry)
    return entry["id"]
    

def delete_key_after_delay(key, delay_ms):
    def delete_key():
        if key in data_store:
            del data_store[key]
            print(f"Key {key} has been deleted")
    delay_seconds = delay_ms / 1000.0
    timer = threading.Timer(delay_seconds, delete_key)
    timer.start()

def parse_resp(data):
    """Parse RESP data into command and arguments."""
    print(data)
    parts = data.decode().split('\r\n')
    print(parts)
    command = None
    args = []
    for i, part in enumerate(parts):
        if part.startswith('*') or part.startswith('$'):
            # Skip lines that start with '*' (arrays) or '$' (bulk strings)
            continue
        elif command is None:
            # The first non-special line is the command
            command = part.lower()
        else:
            # Subsequent lines are arguments
            args.append(part)
    args = args[:-1]
    print("args: ")
    print(args)
    return command, args

def handle_client(conn, addr):
    
    """Handle a client connection."""
    print(f"New connection established from {addr}")
    while True:
        data = conn.recv(1024)
        if not data:
            break

        command, args = parse_resp(data)
# ====================================================================
        if command == "echo":
            message = " ".join(args).strip()
            response = f"${len(message)}\r\n{message}\r\n"
            conn.send(response.encode())
# ====================================================================
        elif command == "ping":
            conn.send(b"+PONG\r\n")
# ====================================================================
        elif command == "set" and len(args) >= 2:
            print("set command called")
            key, value, time = args[0], args[1].strip(), None
            if len(args) >= 4 and args[2].lower().strip() == "px":
                time = int(args[3].strip())
            data_store[key] = {"value": value, "type": "string"}
            if time is not None:
                delete_key_after_delay(key, time)
            conn.send(b"+OK\r\n")
# ====================================================================
        elif command == "get":
            # when get is called: redis-cli get banana ... we expect $9\r\npineapple\r\n
            # but the data_store went from {"banana":"pineapple"} to {"banana": {"value": pineapple, "type": "string"}}
            key = args[0] if args else ""
            item = data_store.get(key, {"value":"", "type": "none"})
            if item["type"] != "none":
                value = item["value"]
                response = f"${len(value)}\r\n{value}\r\n"
            else:
                response = "$-1\r\n"
            conn.send(response.encode())
# ====================================================================
        elif command == "type":
            key = args[0] if args else ""
            if key in data_store:
                data_type = data_store[key]["type"]
                response = f"+{data_type}\r\n"
            else:
                response = conn.send(b"+none\r\n")
            conn.send(response.encode())
# ====================================================================
        elif command == "xadd":
            print("Processing XADD command")  # Debugging print
            if len(args) < 4 or len(args) % 2 != 0: 
                response = "-ERR Wrong number of arguments for 'xadd' command\r\n"
            else:
                stream_key = args[0]
                entry_id = args[1]
                key_value_pairs = args[2:] 
                added_entry_id = addDataStream(stream_key, entry_id, *key_value_pairs)  # Make sure this is correct
                if added_entry_id.startswith("-ERR"):
                    response = added_entry_id # Error response
                else:
                    response = f"${len(added_entry_id)}\r\n{added_entry_id}\r\n"
                print(f"XADD response: {response}")  # Debugging print
                print(data_store)
            conn.send(response.encode())
            print("Response sent for XADD command")  # Debugging print


# ====================================================================
        else:
            print(f"Received unsupported command: {command}")
            # Optionally send an error response to the client
            # conn.send(b"-ERR unsupported command\r\n")

    conn.close()
    print(f"Connection closed with {addr}")

def main():
    """Main function to start the server."""
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is listening on port 6379")
    
    while True:
        conn, addr = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(conn, addr))
        client_thread.start()
        print(f"Active connections: {threading.active_count() - 1}")

if __name__ == "__main__":
    main()
