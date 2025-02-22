import random
import os
from datetime import timedelta

query = "ABCDEFG"
global_rate = {'A': 1639, 'B': 8, 'C': 99, 'D': 11, 'E': 11, 'F': 26, 'G': 957}
network = [[149, 0, 9, 1, 0, 0, 87], [0, 0, 0, 0, 0, 2, 0], [149, 1, 9, 0, 0, 0, 0], [149, 1, 9, 1, 0, 2, 0], [149, 0, 9, 1, 1, 0, 0], [0, 0, 9, 1, 1, 0, 0], [0, 0, 9, 0, 0, 0, 87], [0, 0, 9, 1, 0, 0, 0], [0, 1, 9, 1, 1, 2, 87], [149, 0, 0, 1, 1, 2, 87], [149, 1, 0, 0, 0, 2, 87], [0, 0, 9, 1, 1, 2, 0], [149, 0, 0, 1, 1, 2, 87], [0, 1, 0, 1, 1, 2, 87], [0, 0, 9, 1, 0, 2, 87], [149, 0, 0, 0, 1, 2, 87], [149, 1, 0, 0, 1, 2, 87], [149, 1, 9, 0, 1, 2, 87], [149, 0, 0, 0, 1, 0, 0], [0, 1, 0, 0, 0, 2, 0]]
num_nodes = len(network)

def generate_unique_timestamps(total, max_seconds=3600):
    if total > max_seconds:
        raise ValueError("Total events exceed the available unique seconds.")
    seconds_list = random.sample(range(0, max_seconds), total)
    return [str(timedelta(seconds=s)) for s in seconds_list]

def random_event_id():
    return ''.join(random.choices('0123456789abcdef', k=8))

total_events = sum(global_rate.values())
unique_timestamps = generate_unique_timestamps(total_events, max_seconds=19031)
global_events = []
i = 0
output_dir = "traces"
os.makedirs(output_dir, exist_ok=True)
for event_type, count in global_rate.items():
    for _ in range(count):
        ts = unique_timestamps[i]
        i += 1
        eid = random_event_id()
        global_events.append((ts, event_type, eid))
random.shuffle(global_events)

event_seq_map = {}
for seq, event in enumerate(global_events, start=1):
    _, _, eid = event
    event_seq_map[eid] = seq

with open("traces/global_trace.csv", "w") as f:
    for event in global_events:
        _, et, eid = event
        seq = event_seq_map[eid]
        f.write(f"{et},{seq}\n")

events_by_type = {letter: [] for letter in query}
for event in global_events:
    _, et, _ = event
    if et in events_by_type:
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

for node in node_traces:
    node_traces[node].sort(key=lambda ev: event_seq_map[ev[2]])


for node in range(num_nodes):
    filename = os.path.join(output_dir, f"trace_{node}.csv")
    with open(filename, "w") as f:
        for event in node_traces[node]:
            _, et, eid = event
            seq = event_seq_map[eid]
            f.write(f"{et},{seq}\n")
    print(f"Wrote {len(node_traces[node])} events to {filename}")
