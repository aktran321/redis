import socket
import threading

datastore = {}

def parse_resp(data):
    # Simplified parsing logic for illustration; real implementation should be more robust
    parts = data.decode().split('\r\n')
    if parts[0] == '*2':  # Basic handling for two-part commands like ECHO
        command = parts[2].lower()  # Command name
        message = parts[4] if len(parts) > 4 else ''  # Command argument
        return command, message
    return parts[2].lower(), None  # For commands without arguments like PING

def handle_client(conn, addr):
    global datastore

    print(f"New connection established from {addr}")
    while True:
        data = conn.recv(1024)
        if not data:
            break

        command, message = parse_resp(data)
        args = message.split()
        
        if command == "echo":
            response = f"${len(message)}\r\n{message}\r\n"
            conn.send(response.encode())
        elif command == "ping":
            conn.send(b"+PONG\r\n")
        elif command == "set" and len(args) >= 2:
            key, value = args[0], args[1]
            datastore[key] = value
            conn.send(b"+OK\r\n")
        elif command == "get" and len(args) >= 1:
            key = args[0]
            value = datastore.get(key) # do this incase there is no value for the key that is inputted... avoiding your program to crash
            if value is not None:
                response = f"${len(value)}\r\n{value}\r\n"
            else:
                response = f"$-1\r\n"
            conn.send(response.encode())

        else:
            print(f"Received unsupported command: {command}")
            # Break or handle unsupported commands differently
    conn.close()
    print(f"Connection closed with {addr}")

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is listening on port 6379")
    
    while True:
        conn, addr = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(conn, addr))
        client_thread.start()
        print(f"Active connections: {threading.active_count() - 1}")

if __name__ == "__main__":
    main()
