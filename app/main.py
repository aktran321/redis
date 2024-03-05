import socket
import threading

def parse_resp(data):
    try:
        lines = data.split(b'\r\n')
        # Checks to ensure that the expected parts of the message are present
        if len(lines) >= 5:
            command = lines[2].decode().lower()
            message = lines[4].decode()
            return command, message
        else:
            return None, None  # Returns None if the command format is not as expected
    except IndexError as e:
        print(f"Error parsing data: {e}")
        return None, None
    except Exception as e:
        print(f"Unexpected error parsing data: {e}")
        return None, None

def handle_client(conn, addr):
    print(f"New connection established from {addr}")
    while True:
        data = conn.recv(1024)
        if not data:
            break
        
        command, message = parse_resp(data)
        
        # Handling PING command
        if command == "ping":
            response = "+PONG\r\n"
            conn.send(response.encode())
        # Handling ECHO command
        elif command == "echo" and message is not None:
            response = f"${len(message)}\r\n{message}\r\n"
            conn.send(response.encode())
        else:
            print("Received malformed or unsupported command")
            # Optional: Close the connection if command is malformed or unsupported
            # conn.close()
            # break

    conn.close()
    print(f"Connection with {addr} closed")


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is listening on port 6379")
    
    while True:
        conn, addr = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(conn, addr))
        client_thread.start()
        # Updated to use the non-deprecated method active_count() instead of activeCount()
        print(f"Active connections: {threading.active_count() - 1}")

if __name__ == "__main__":
    main()
