# Uncomment this to pass the first stage
import socket
import threading

def parse_resp(data):
    lines = data.split(b'\r\n')
    command = lines[2].decode().lower()
    message = lines[4].decode()
    return command, message

def handle_client(conn, addr):
    """
    handles a single client connection
    """
    print(f"New connection established from {addr}")
    while True:
        data = conn.recv(1024)
        if not data:
            break
        command, message = parse_resp(data)
        if command == "echo":
            response = f"${len(message)}\r\n{message}\r\n"
            conn.send(response.encode())
    conn.close()


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("server is listening on port 6379")
    print("Server is listening for connections")
    
    while True:
        conn, addr = server_socket.accept()

        client_thread = threading.Thread(target=handle_client, args = (conn, addr))
        client_thread.start()
        print(f"Activate connections: {threading.activeCount() - 1}")

if __name__ == "__main__":
    main()
