import csv
import datetime
import re

def main():
    query = "ABCDEFG"
    complete_query_aggregates = 0
    prefix = "traces/trace_"
    id_list = [0, 6, 8, 9, 10, 12, 13, 14, 15, 16, 17]

    for idx in id_list:
        filename = prefix + query + "_" + str(idx) + ".csv"
        with open(filename, mode='r', newline='') as infile:
            reader = csv.reader(infile)
            for row in reader:
                myfields = ["", "", "", ""]
                myfields[0], myfields[1], myfields[2], myfields[3] = row
                complete_query_aggregates += int(myfields[2])

    with open("final_global_aggregates.txt", "r") as file:
        global_aggregate_count = int(file.readline().split(" ")[3])
    print("global_query_aggregates   =", global_aggregate_count)
    print("complete_query_aggregates =", complete_query_aggregates)

    with open('final_aggregates.txt', 'w') as file:
        file.write("Global trace accumulated " + str(complete_query_aggregates) +
                   " aggregates " + query + "\n")

    with open("messages_of_each_node.txt", "r") as infile, \
         open("messages_in_total.txt", "w") as outfile:
        total_messages = 0
        times = []
        # This regex finds time strings in the format H:MM:SS.microseconds.
        time_pattern = re.compile(r'(\d+:\d+:\d+\.\d+)')
        for line in infile:
            if line.startswith("Messages sent"):
                parts = line.split()
                total_messages += int(parts[-1])
            elif line.startswith("Time when Node"):
                match = time_pattern.search(line)
                if match:
                    time_str = match.group(1)
                    h, m, s = time_str.split(":")
                    time_sec = int(h) * 3600 + int(m) * 60 + float(s)
                    times.append(time_sec)

        if times:
            earliest = min(times)
            latest = max(times)
            duration = latest - earliest
        else:
            earliest = latest = duration = 0

        print("Total messages sent:", total_messages)
        print("Earliest time:", earliest)
        print("Latest time:", latest)
        print("Duration:", duration)
        outfile.write("Total messages sent: " + str(total_messages) + "\n")
        outfile.write("Earliest time: " + str(earliest) + "\n")
        outfile.write("Latest time: " + str(latest) + "\n")
        outfile.write("Duration: " + str(duration))

if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
