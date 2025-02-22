import csv
import subprocess
import os

def main():
    query = "ABCDEFG"
    complete_query_aggregates = 0
    input_file = "traces/global_trace.csv"
    global_output_files = "global_output/trace_"

    input_stream = ""
    with open(input_file, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            input_stream += str(row[0]) + " " + str(row[1]) + " 1 " + str(row[0]) + " "

    input_stream = input_stream[:-1]   # remove potentially annoying space at the end
    # print(input_stream)

    executable_path = "./sequence_accumulator"
    for i in range(1,len(query) ):
        options = ["-t", query[i], query[:i+1]]
        command = [executable_path] + options
        process = subprocess.run(command, input=input_stream, text=True, capture_output=True)
        output_stream = process.stdout
        elements = output_stream.split()
        groups = [elements[i:i+4] for i in range(0, len(elements), 4)]
        with open(global_output_files+query[:i+1]+".csv", "w") as file:
            for group in groups:
                group = [group[3], group[1], group[2], group[0]]
                if int(group[2]) > 0:
                    file.write(','.join(group) + '\n')

    options = ["-t", query[-1], query]
    command = [executable_path] + options
    process = subprocess.run(command, input=input_stream, text=True, capture_output=True)

    output_stream = process.stdout

    #print(output_stream)
    elements = output_stream.split()
    groups = [elements[i:i+4] for i in range(0, len(elements), 4)]
    for group in groups:
        if int(group[2]) > 0 and group[3] == query:
            complete_query_aggregates += int(group[2])

    print("complete_query_aggregates =", complete_query_aggregates)
    with open('final_global_aggregates.txt', 'w') as file:
        file.write("Global trace accumulated " + str(complete_query_aggregates) + " aggregates " + query + "\n")

if __name__ == "__main__":
    main()
