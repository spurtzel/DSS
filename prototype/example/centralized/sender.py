import csv
import datetime
import json
import socket
import subprocess
import sys
import threading
import time
import os

query = "ABCDEF"
buffer_size = 256 * 1024
base_port_value = 60600
plans_prefix = 'plans/config_'
trace_prefix = 'traces/trace_'
log_prefix = 'logs/log_'
message_counter_file = "messages_of_each_node.txt"

event_types_sent = []
number_of_sent_messages = 0
eof_sent = []
aggregates_computed = []
my_dicts = {}

# keep a dictionary of persistent connections to other nodes:
outgoing_sockets = {}  # maps node_id -> (socket_object, file_object_for_writing)

def send_events(csv_file_path, forwarding, myid, processing):
    global event_types_sent
    global number_of_sent_messages
    global eof_sent
    global my_dicts
    global outgoing_sockets 

    locally_generated_inputs = {}
    target_aggregates = [forwarding[x][1] for x in forwarding.keys()]
    print("S: target_aggregates:", target_aggregates)
    for aggregate in target_aggregates:
        locally_generated_inputs[aggregate] = [x for x in forwarding.keys() if aggregate == forwarding[x][1]]
    print("S: locally_generated_inputs:", locally_generated_inputs)

    with open(csv_file_path, 'r') as file:
        csv_reader = csv.reader(file)

        for row in csv_reader:
            if len(row) == 2:
                event_type, timestamp = row
                message = f"{event_type} {timestamp} 1 {event_type}"
            else:
                event_type, timestamp, count, rate = row
                message = f"{event_type} {timestamp} {count} {rate}"

            if event_type not in event_types_sent:
                event_types_sent.append(event_type)

            if event_type in forwarding:
                node_ids = forwarding[event_type][0]
                for node_id in node_ids:
                    port = base_port_value + node_id
                    if myid != node_id:
                        number_of_sent_messages += 1
                        # Log
                        with open(log_prefix + str(myid) + '.csv', 'a') as f:
                            f.write(message + '\n')

                        if node_id not in outgoing_sockets:
                            # open the TCP connection once and store it
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.connect(("localhost", port))
                            sock_file = sock.makefile('w', encoding='utf-8')
                            outgoing_sockets[node_id] = (sock, sock_file)
                        else:
                            sock, sock_file = outgoing_sockets[node_id]

                        # write the message + newline, then flush
                        sock_file.write(message + "\n")
                        sock_file.flush()

                    else:
                        # write local events directly to dict
                        myfields = message.split(" ")
                        ts = int(myfields[1])
                        count = int(myfields[2])
                        rate = myfields[3]
                        aggregate = forwarding[event_type][1]
                        if ts in my_dicts[aggregate].keys():
                            my_dicts[aggregate][ts][2] += count
                        else:
                            my_dicts[aggregate][ts] = [event_type, ts, count, rate]

    print("S: event_types_sent:", event_types_sent)

    my_eof_messages = {}
    for aggregate in target_aggregates:
        my_eof_messages[aggregate] = []
        for key in forwarding.keys():
            if aggregate == forwarding[key][1]:
                for node_id in forwarding[key][0]:
                    my_eof_messages[aggregate].append((node_id, key))
    print(f"S: Full eos_messages for node {str(myid)}:", my_eof_messages, f"(after {csv_file_path})")

    if os.path.getsize(csv_file_path) == 0:
        event_types_sent.append(csv_file_path.split("_")[1])

    for aggregate in my_eof_messages.keys():
        if set(locally_generated_inputs[aggregate]).issubset(set(event_types_sent)) and aggregate not in eof_sent:
            eof_sent.append(aggregate)
            for node_id, sent_type in my_eof_messages[aggregate]:
                message = aggregate + " END-OF-STREAM " + sent_type + " " + str(myid) + " " + str(node_id)
                message_bytes = message.encode('utf-8')
                port = base_port_value + node_id
                print("S: sending", message, "to", node_id)

                # use the same persistent connection
                if node_id not in outgoing_sockets:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(("localhost", port))
                    sock_file = sock.makefile('w', encoding='utf-8')
                    outgoing_sockets[node_id] = (sock, sock_file)
                else:
                    sock, sock_file = outgoing_sockets[node_id]

                sock_file.write(message + "\n")
                sock_file.flush()

                with open(log_prefix + str(myid) + '.csv', 'a') as f:
                    f.write(message + '\n')

    if not processing:
        print("F:", myid, "has sent", number_of_sent_messages, "messages in total (type: source only)")


def only_source(processing):
    if not processing:
        return True

def get_aggregate(eventtype, processing):
    for aggregate in processing:
        if eventtype in processing[aggregate][2]:
            return aggregate

def handle_incoming_connection(conn, myid, processing, forwarding, eof_counter, predecessors, all_aggregates, lock):
    global my_dicts
    global aggregates_computed

    with conn:
        file_obj = conn.makefile('r', encoding='utf-8')
        while True:
            line = file_obj.readline()
            if not line:
                # connection closed on other side
                break
            decoded_message = line.strip()
            if not decoded_message:
                continue

            # Log message
            with open(log_prefix + str(myid) + '.csv', 'a') as f:
                f.write(decoded_message + "," + '\n')

            myfields = decoded_message.split(" ")

            if len(myfields) < 2:
                continue  

            if myfields[1] == "END-OF-STREAM":
                # format: "<aggregate> END-OF-STREAM <sent_type> <source_node> <target_node>"
                aggregate_received = myfields[0]
                source_node = myfields[3]

                print("L: Received EOS for", aggregate_received, "from", source_node, "sending", myfields[2])
                with lock:
                    if aggregate_received in eof_counter:
                        eof_counter[aggregate_received] += 1

                    # check if we can process aggregates
                    for aggregate in processing.keys():
                        if aggregate not in all_aggregates and eof_counter[aggregate] == predecessors[aggregate]:
                            all_aggregates.append(aggregate)

                    for aggregate in processing.keys():
                        if (eof_counter[aggregate] == predecessors[aggregate] and
                                aggregate not in aggregates_computed):
                            print("L: eof_counter:", sum(eof_counter.values()), "|", eof_counter)
                            print("L: predecessors:", sum(predecessors.values()), "|", predecessors)

                            aggregates_computed.append(aggregate)
                            received_events = []
                            for ts in sorted(my_dicts[aggregate].keys()):
                                # if message contains an aggregate
                                if len(my_dicts[aggregate][ts][0]) > 1:
                                    my_dicts[aggregate][ts][0], my_dicts[aggregate][ts][-1] = \
                                        my_dicts[aggregate][ts][-1], my_dicts[aggregate][ts][0]
                                received_events.extend(map(str, my_dicts[aggregate][ts]))

                            input_stream = ' '.join(received_events)
                            print("P: AGGREGATE", aggregate, "has input_stream:", input_stream[:512])

                            executable_path = "./sequence_accumulator"
                            sequence_pattern = aggregate
                            output_eventtype = processing[aggregate][1]

                            options = ["-t", output_eventtype, sequence_pattern]
                            command = [executable_path] + options

                            process = subprocess.run(command, input=input_stream, text=True, capture_output=True)
                            output_stream = process.stdout
                            elements = output_stream.split()
                            groups = [elements[i:i+4] for i in range(0, len(elements), 4)]

                            with open(trace_prefix + aggregate + "_" + str(myid) + '.csv', 'w') as f:
                                for group in groups:
                                    group = [group[3], group[1], group[2], group[0]]
                                    if int(group[2]) > 0:
                                        f.write(','.join(group) + '\n')

                            send_events(trace_prefix + aggregate + "_" + str(myid) + '.csv',
                                        forwarding, myid, processing)

            else:
				# primitive or complex event: i.e., "<eventtype> <timestamp> <count> <rate>"
                if len(myfields) < 4: 
                    continue
                eventtype = myfields[0]
                myfields[1] = int(myfields[1])
                myfields[2] = int(myfields[2])
                aggregate = get_aggregate(eventtype, processing)

                with lock:
                    if myfields[1] not in my_dicts[aggregate]:
                        my_dicts[aggregate][myfields[1]] = myfields
                    else:
                        my_dicts[aggregate][myfields[1]][2] += myfields[2]

def listen_to_port(myid, processing, forwarding):
    global my_dicts
    global aggregates_computed

    if only_source(processing):
        return
    else:
        eof_counter = {}
        predecessors = {}
        for aggregate in processing.keys():
            eof_counter[aggregate] = 0
            predecessors[aggregate] = processing[aggregate][3]

    all_aggregates = []

    port = base_port_value + myid
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buffer_size)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buffer_size)
    sock.bind(('', port))
    sock.listen()

    sock.settimeout(1.0)  # periodically check if we are done
    print(f"Listening on port {port}...\n")

    lock = threading.Lock()  # to synchronize updates to eof_counter, my_dicts, etc.

    listening = True
    while listening:
        try:
            conn, addr = sock.accept()
            # spawn a thread to handle this connection
            t = threading.Thread(
                target=handle_incoming_connection,
                args=(conn, myid, processing, forwarding, eof_counter, predecessors, all_aggregates, lock),
                daemon=True
            )
            t.start()
        except socket.timeout:
            pass

        # check if all aggregates have reached their EOF
        with lock:
            if sum(eof_counter.values()) >= sum(predecessors.values()):
                listening = False

    print("F: final eof_counter:", sum(eof_counter.values()), "|", eof_counter)
    print("F: final predecessors:", sum(predecessors.values()), "|", predecessors)
    print("Stopped listening.")
    sock.close()


def main():
    if len(sys.argv) < 2:
        myid = 1
    else:
        myid = int(sys.argv[1])

    csv_file_path = trace_prefix + str(myid) + '.csv'

    if myid == 0:
        open(message_counter_file, 'w').close()

    with open(log_prefix + str(myid) + '.csv', 'w') as f:
        f.write("Received Messages, Sent Messages\n")

    with open(plans_prefix + str(myid) + '.json', 'r') as file:
        data = json.load(file)

    globals().update(data)
    global my_dicts
    for aggregate in processing.keys():
        my_dicts[aggregate] = {}

    listening_thread = threading.Thread(target=listen_to_port, args=(myid, processing, forwarding))
    listening_thread.start()

    now = datetime.datetime.now()
    wait_seconds = 60 - now.second
    print(f"Waiting for {wait_seconds} seconds to start at the next full minute...")
    time.sleep(wait_seconds)
    start = datetime.datetime.now()
    send_events(csv_file_path, forwarding, myid, processing)

    listening_thread.join()

    # close all persistent outgoing connections
    global outgoing_sockets
    for node_id, (sock, file_obj) in outgoing_sockets.items():
        try:
            file_obj.close()
        except:
            pass
        try:
            sock.close()
        except:
            pass

    with open(message_counter_file, 'a') as file:
        file.write("Messages sent by Node " + str(myid) + ": " + str(number_of_sent_messages) + "\n")
        file.write("Time when Node " + str(myid) + ": " + str(start) + " started \n")
        file.write("Time when Node " + str(myid) + ": " + str(datetime.datetime.now()) + " finished \n")
	
if __name__ == "__main__":
    main()
