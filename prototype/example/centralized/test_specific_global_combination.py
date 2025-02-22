import csv
import subprocess
import os

def main():
    query = "ABCDEFGH"
    cut_event = "B"
    input_aggregates = ["AB", "CDEFGH"]
    input_prefix = "global_output/trace_"
    global_output_files = "global_output/trace_" + query
    complete_query_aggregates = 0

    my_dicts = {}
    for aggregate in input_aggregates:
        with open(input_prefix+aggregate+".csv", "r") as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                myfields = ["","","",""]
                myfields[0], myfields[1], myfields[2], myfields[3] = row
                myfields[1] = int(myfields[1])
                myfields[2] = int(myfields[2])
                if not int(myfields[1]) in my_dicts.keys():
                    my_dicts[int(myfields[1])] = myfields
                else:
                    my_dicts[int(myfields[1])][2] += int(myfields[2])

    received_events = []
    for event in sorted(my_dicts.keys()):  # sorted by timestamps
        # if message contains a projection aggregate
        if len(my_dicts[event][0]) > 1:
            # swap the first and last elements
            my_dicts[event][0], my_dicts[event][-1] = my_dicts[event][-1], my_dicts[event][0]
        received_events.extend(map(str, my_dicts[event]))

    input_stream = ' '.join(received_events)
    input_stream = input_stream[:-1]   # remove potentially annoying space at the end
    # print(input_stream)

    executable_path = "./sequence_accumulator_fat"

    options = ["-t", cut_event, query]
    command = [executable_path] + options
    process = subprocess.run(command, input=input_stream, text=True, capture_output=True)

    output_stream = process.stdout

    #print(output_stream)
    elements = output_stream.split()
    groups = [elements[i:i+4] for i in range(0, len(elements), 4)]
    with open(global_output_files+"_new.csv", 'w') as f:
        for group in groups:
            group = [group[3], group[1], group[2], group[0]]
            if int(group[2]) > 0:
                f.write(','.join(group) + '\n')
                if group[0] == query:
                    complete_query_aggregates += int(group[2])

    print("complete_query_aggregates =", complete_query_aggregates)

if __name__ == "__main__":
    main()
