import glob
import csv
from datetime import datetime

def parse_time(t):
    return datetime.strptime(t, '%H:%M:%S')

def main():
    events = []

    for filename in glob.glob("*.csv"):
        with open(filename, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) < 4:
                    continue
                time_str, event_type, _, count = row
                events.append((parse_time(time_str), event_type, count))
    
    events.sort(key=lambda x: x[0])
    
    output = ''
    for i, (_, event_type, count) in enumerate(events, start=1):
        output +=  event_type + ' ' + str(i) + ' ' + str(count) + ' ' + event_type + ' '
    print(output)

if __name__ == '__main__':
    main()
