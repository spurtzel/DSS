import random
import os
from datetime import timedelta, datetime


query = "ABCD"

global_rate = {'A': 5000, 'B': 10, 'C': 7000, 'D': 21}


network = [
    [0, 0, 3500, 7],
    [0, 10, 0,    0],
    [5000, 0, 0,    7],
    [0, 0, 3500, 0],
    [0, 0, 0,    7]
]

num_nodes = len(network)


def generate_unique_timestamps(total, max_seconds=3600):
    if total > max_seconds:
        raise ValueError("Total events exceed the available unique seconds.")
   
    seconds_list = random.sample(range(0, max_seconds), total)
    return [str(timedelta(seconds=s)) for s in seconds_list]

def random_event_id():
    return ''.join(random.choices('0123456789abcdef', k=8))

def format_event(timestamp, event_type, event_id):
    return f"{timestamp},{event_type},{event_id},1"


total_events = sum(global_rate.values())
unique_timestamps = generate_unique_timestamps(total_events, max_seconds=25000)

global_events = []
i = 0
for event_type, count in global_rate.items():
    for _ in range(count):
        ts = unique_timestamps[i]
        i += 1
        eid = random_event_id()
        global_events.append((ts, event_type, eid))

random.shuffle(global_events)

events_by_type = {letter: [] for letter in query}
for event in global_events:
    ts, et, eid = event
    events_by_type[et].append(event)

for et in events_by_type:
    random.shuffle(events_by_type[et])

node_traces = {i: [] for i in range(num_nodes)}
for col, letter in enumerate(query):
    pool = events_by_type[letter]
    total_needed = sum(network[node][col] for node in range(num_nodes))
    if len(pool) != total_needed:
        print(f"Warning: For event type {letter}, pool size {len(pool)} does not match total needed {total_needed}")
    start_index = 0
    for node in range(num_nodes):
        count = network[node][col]
        assigned = pool[start_index:start_index+count]
        node_traces[node].extend(assigned)
        start_index += count


def parse_timestamp(ts_str):
    return datetime.strptime(ts_str, "%H:%M:%S")

for node in node_traces:
    node_traces[node].sort(key=lambda ev: parse_timestamp(ev[0]))


output_dir = "traces"
os.makedirs(output_dir, exist_ok=True)

for node in range(num_nodes):
    filename = os.path.join(output_dir, f"trace_{node}.csv")
    with open(filename, "w") as f:
        for event in node_traces[node]:
            ts, et, eid = event
            f.write(format_event(ts, et, eid) + "\n")
    print(f"Wrote {len(node_traces[node])} events to {filename}")
