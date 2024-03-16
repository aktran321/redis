import socket
import threading
import time

data_arrival_condition = threading.Condition()
# Initialize the data_store for storing key-value pairs
data_store = {}
def createXreadResponse(dType, stream_key, id):
    combined_response = ""
    # now lets split the ID again
    # [streams, stream_key, 0-0]
    ms_id, seq_id = map(int, id.split("-"))
    if data_store[stream_key]["type"] == dType or dType =="streams" or dType == "stream":
        # then okay lets start building a response...
        response = ""
        nu_of_valid_entries = 0
        for entry in data_store[stream_key]["value"]:
            # looping through the array of entries
            # must validate the id's
            entry_id = entry["id"] 
            entry_ms, entry_seq = map(int, entry_id.split("-"))
            if entry_seq < seq_id or (entry_seq == seq_id and entry_ms <= ms_id):
                # if the id is out of range, dont include... move on
                continue
            else:
                nu_of_valid_entries += 1
                # now we have to construct combined response
                # this is for the array with key/value pairs for elements, which will be a variable even number
                
                combined_response = ""
                entry_response = ""
                nu_of_kv = 0
                
                for key, value in entry.items():
                    if key != "id":
                        # append the key and value 
                        nu_of_kv += 2
                        entry_response += f"${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n" #  $11\r\n$temperature\r\n$2\r\n96\r\n
                    else:
                        continue
                entry_response = f"*{nu_of_kv}\r\n" + entry_response # *2
                combined_response += f"*2\r\n${len(entry_id)}\r\n{entry_id}\r\n" + entry_response
                
        if combined_response:
            response = f"*2\r\n${len(stream_key)}\r\n{stream_key}\r\n*{nu_of_valid_entries}\r\n" + combined_response
        else:
            response = f"$-1\r\n"
        print(response)
    return response

def addDataStream(stream_key, entry_id, *key_value_pairs):
    global data_arrival_condition 
    if stream_key not in data_store:
        data_store[stream_key] = {"type": "stream", "value": []}
    last_entry_id = data_store[stream_key]["value"][-1]["id"] if data_store[stream_key]["value"] else "0-0"
    last_ms, last_seq = map(int, last_entry_id.split("-"))

    if entry_id == "*":
        current_ms = int(time.time() * 1000)
        current_seq = last_seq + 1 if last_ms == current_ms else 0 # Increment seq if ms are the same
        entry_id = f"{current_ms}-{current_seq}"
    else:
        parts = entry_id.split("-")
        current_ms = int(parts[0]) if parts[0].isdigit() else int(time.time() * 1000)
        current_seq = int(parts[1]) if len(parts) > 1 and parts[1] != "*" else last_seq + 1 if last_ms == current_ms else 0
        entry_id = f"{current_ms}-{current_seq}"

    if current_ms == 0 and current_seq == 0:
        return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
    if current_ms < last_ms or (current_ms == last_ms and current_seq <= last_seq):
        return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
    entry = {"id": entry_id}
    for i in range(0, len(key_value_pairs), 2):
        key = key_value_pairs[i]
        value = key_value_pairs[i+1]
        entry[key] = value
    with data_arrival_condition:
        data_store[stream_key]["value"].append(entry)
        data_arrival_condition.notify_all()
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
    command = parts[2].lower()
    args = []


    lookingForAst = command == "xadd"
    astFound = False

    for i, part in enumerate(parts):
        if lookingForAst and part == "*" and not astFound:
            args.append(part)
            astFound = True
            continue # Skip the rest of the loop for this iteration
        if part.startswith('*') or part.startswith('$'):
            # Skip lines that start with '*' (arrays) or '$' (bulk strings)
            continue
        elif i < 3:
            continue
        else:
            # Subsequent lines are arguments
            args.append(part)
    if args and args[-1] == "":
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
            key, value, delete_time = args[0], args[1].strip(), None
            if len(args) >= 4 and args[2].lower().strip() == "px":
                delete_time = int(args[3].strip())
            data_store[key] = {"value": value, "type": "string"}
            if delete_time is not None:
                delete_key_after_delay(key, delete_time)
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
        # now we have to account for the "-" for args[1] and args[2]
        elif command == "xrange":
            key, id1, id2 = args[0], args[1], args[2]
            if id1 == "-":
                time_id_1, seq_id_1 = 0, 0
            elif "-" not in id1:
                time_id_1 = int(id1)
                seq_id_1 = 0
            else:
                time_id_1, seq_id_1 = map(int, args[1].split("-"))
            if  id2 == "+":
                time_id_2, seq_id_2 = 9999999999999, 9
            elif "-" not in id2:
                time_id_2 = int(id2)
                seq_id_2 = 9
            else:
                time_id_2, seq_id_2 = map(int, args[2].split("-"))

            list_of_entries = data_store[key]["value"]

            
            # ====================================================================
            # ====================================================================
            # now we want to construct our response. 
            number_of_stream_entries_in_range = 0
            beta_response = ""
            for i in range(len(list_of_entries)): # loop through our whole stream

                # check the ID of the stream
                entry_id = list_of_entries[i]["id"]

                curr_time_id, curr_seq_id = map(int, entry_id.split("-"))
                if seq_id_1 <= curr_seq_id <= seq_id_2 and time_id_1 <= curr_time_id <= time_id_2:
                    print("SCRIPT FOUND AN ENTRY IN RANGE")
                    number_of_stream_entries_in_range += 1

                    # *2\r\n$15\r\n{""}\r\n
                    first_entry_response = f"*2\r\n"

                    # *4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n
                    second_entry_response = f""
                    inner_list_counter = 0
                    for key, value in list_of_entries[i].items():
                        if key == "id":
                            length = len(value)
                            first_entry_response += f"${length}\r\n{value}\r\n"
                        else:
                            inner_list_counter += 2
                            second_entry_response += f"${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
                    # *4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n
                    second_entry_response = f"*{inner_list_counter}\r\n" + second_entry_response

                    # once we are done looping the key/value pairs for the entry that fits in our range
                    # we combine the id and key/val pair responses
                    combined_response_for_one_entry = first_entry_response + second_entry_response

                    # then we concatenate them to our beta response...
                    # this concatenation happens everytime we find a ID in our range. 
                    beta_response += (combined_response_for_one_entry)

            # once we are finished looping through the array of entries, and found all of our IDs in range and concatenated them to beta_reponse
            # we create the full response
            response = f"*{number_of_stream_entries_in_range}\r\n" + beta_response
            print("Full response for xrange: ")
            print(response)
            
            conn.send(response.encode())
            print("Response for xrange sent")
            # response = f"*2\r\n*2\r\n$15\r\n{""}\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n{""}-9\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n"
            
        # ====================================================================
        elif command == "xread":
            # Single stream read
            if len(args) == 3:
                dType, stream_key, id = args[0], args[1], args[2]
                response = f"*1\r\n" + createXreadResponse(dType, stream_key, id)

            # example data) 
            #{'stream_key': {'type': 'stream', 'value': [{'id': '0-1', 'temperature': '95'}]}, 'other_stream_key': {'type': 'stream', 'value': [{'id': '0-2', 'humidity': '97'}]}}
            # example args
            # 
            elif len(args) == 5 and args[0] == "block":
                wait_time, dType, stream_key, id = int(args[1]), args[2], args[3], args[4]
                if id == "$":
                    # we want the id to be the most recent data with in the stream... sooo
                    if stream_key in data_store and data_store[stream_key]["value"]:
                        id = data_store[stream_key]["value"][-1]["id"]
                        print("changed the value of ID")
                    else:
                        id = None
                end_time = time.time() + wait_time / 1000.0 if wait_time != 0 else None
                print("block xread hit")
                print("End Time: ", end_time)

                with data_arrival_condition:
                    print("inside with data_arrival_condition boolean")
                    while True:
                        if end_time and time.time() >= end_time:
                            print("time has passed the end_time, so we are breaking the while loop")
                            response = "$-1\r\n"
                            break
                        remaining_time = end_time - time.time() if end_time else None
                        if not remaining_time or remaining_time > 0:
                            data_arrival_condition.wait(timeout=remaining_time)
                            print("We have WOKEN UP")
                            response = f"*1\r\n" + createXreadResponse(dType, stream_key, id)
                            print(response)
                            break
                        else:
                            print("break for no reason")
                            response = "$-1\r\n"
                            break
            
            elif len(args) == 5:
                print("this should not be hitting if we use the BLOCK command")
                dType, key1, key2, id1, id2 = args[0], args[1], args[2], args[3], args[4]
                response1, response2 = createXreadResponse(dType, key1, id1), createXreadResponse(dType, key2, id2)
                response = f"*2\r\n" + response1 + response2
            else:
                response = "-ERR invalid number of arguments"
            conn.send(response.encode())




                        

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
