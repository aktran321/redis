import socket
import threading

# Initialize the datastore for storing key-value pairs
datastore = {}

def parse_resp(data):
    """Parse RESP data into command and arguments."""
    parts = data.decode().split('\r\n')
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
    return command, args

def handle_client(conn, addr):
    """Handle a client connection."""
    print(f"New connection established from {addr}")
    while True:
        data = conn.recv(1024)
        if not data:
            break

        command, args = parse_resp(data)

        if command == "echo":
            message = " ".join(args)
            response = f"${len(message)}\r\n{message}\r\n"
            conn.send(response.encode())
        elif command == "ping":
            conn.send(b"+PONG\r\n")
        elif command == "set" and len(args) >= 2:
            key, value = args[0], " ".join(args[1:])
            datastore[key] = value
            conn.send(b"+OK\r\n")
        elif command == "get":
            key = args[0] if args else ""
            value = datastore.get(key)
            if value is not None:
                response = f"${len(value)}\r\n{value}\r\n"
            else:
                response = "$-1\r\n"
            conn.send(response.encode())
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
