# Uncomment this to pass the first stage
import socket
import threading

def handle_client(conn, addr):
    """
    handles a single client connection
    """
    print("New connection established from {addr}")
    while True:
        data = conn.recv(1024)
        if not data:
            break
        conn.send(b"+PONG\r\n")
    conn.close()
    print("Connection closed with {addr}")


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("server is listening on port 6379")
    server_socket.listen()
    print("Server is listening for connections")
    
    while True:
        conn, addr = server_socket.accept()

        client_thread = threading.Thread(target=handle_client, args = (conn, addr))
        client_thread.start()
        print(f"Activate connections: {threading.activeCount() - 1}")

if __name__ == "__main__":
    main()
