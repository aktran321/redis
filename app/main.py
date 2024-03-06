import socket
import threading

# Initialize the datastore for storing key-value pairs
datastore = {}

def delete_key_after_delay(key, delay_ms):
    def delete_key():
        if key in datastore:
            del datastore[key]
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
            datastore[key] = value
            if time is not None:
                delete_key_after_delay(key, time)
            conn.send(b"+OK\r\n")
# ====================================================================
        elif command == "get":
            key = args[0] if args else ""
            value = datastore.get(key, None)
            if value is not None:
                response = f"${len(value)}\r\n{value}\r\n"
            else:
                response = "$-1\r\n"
            conn.send(response.encode())
# ====================================================================
        elif command == "type":
            key = args[0] if args else ""
            if key in datastore:
                conn.send(b"+string\r\n") 
            else:
                return conn.send(b"+none\r\n")
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
