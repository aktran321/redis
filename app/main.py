# Uncomment this to pass the first stage
import socket


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("server is listening on port 6379")
    conn , _ = server_socket.accept()
    print("connection established")
    while True:
        # receive data from the client
        data = conn.recv(1024)
        if not data:
            # if no data is received, break from the loop
            break
        # send response to client
        conn.send(b"+PONG\r\n")
    conn.close()
    print("connection closed")

if __name__ == "__main__":
    main()
