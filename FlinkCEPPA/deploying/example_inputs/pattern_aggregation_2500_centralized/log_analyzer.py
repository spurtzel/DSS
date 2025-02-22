import sys
import datetime
import subprocess

def parse_timestamp(ts_str):
    # Supports "HH:MM:SS.fraction" or "HH:MM:SS:microseconds"
    if '.' in ts_str:
        hh_mm_ss, fraction = ts_str.split('.', 1)
        parts = hh_mm_ss.split(':')
        if len(parts) != 3:
            raise ValueError(f"Unexpected timestamp format: {ts_str}")
        hour, minute, second = map(int, parts)
        micro_str = fraction[:6].ljust(6, '0')
        return datetime.timedelta(hours=hour, minutes=minute, seconds=second, microseconds=int(micro_str))
    else:
        parts = ts_str.split(':')
        if len(parts) != 4:
            raise ValueError(f"Unexpected timestamp format: {ts_str}")
        hour, minute, second, micro_str = parts
        return datetime.timedelta(hours=int(hour), minutes=int(minute), seconds=int(second), microseconds=int(micro_str))

def main():
    # Redirect output to "result.txt"
    original_stdout = sys.stdout
    with open("result.txt", "w") as out_file:
        sys.stdout = out_file

        if len(sys.argv) != 2:
            print("Usage: python analyzer.py <n>")
            sys.exit(1)
        n = int(sys.argv[1])
        earliest = None
        earliest_file = None
        latest = None
        latest_file = None

        # Initialize stats per file.
        # Note: fwp_product_sum will accumulate aggregate values extracted from
        # FinalWindowProcessFunction lines only.
        stats = {}
        for i in range(n):
            filename = f"{i}.log"
            stats[filename] = {
                "msg_received_count": 0,
                "complex_event_count": 0,
                "fwp_product_sum": 0
            }
            try:
                with open(filename, "r") as file:
                    for line in file:
                        line = line.rstrip('\n')

                        # Process "was received at:" lines.
                        if "was received at:" in line:
                            index = line.find("was received at:")
                            ts_part = line[index + len("was received at:"):].strip()
                            if ts_part:
                                first_token = ts_part.split()[0]
                                try:
                                    ts = parse_timestamp(first_token)
                                    if earliest is None or ts < earliest:
                                        earliest = ts
                                        earliest_file = filename
                                except ValueError as e:
                                    print(f"Error parsing earliest timestamp in {filename}: '{first_token}' - {e}")
                            tokens = line.strip().split()
                            if tokens:
                                last_token = tokens[-1]
                                try:
                                    parse_timestamp(last_token)
                                    stats[filename]["msg_received_count"] += 1
                                except ValueError:
                                    pass

                        # Process FinalWindowProcessFunction lines.
                        if "FinalWindowProcessFunction" in line:
                            tokens = line.strip().split()
                            if tokens:
                                last_token = tokens[-1]
                                try:
                                    ts = parse_timestamp(last_token)
                                    if latest is None or ts > latest:
                                        latest = ts
                                        latest_file = filename
                                except ValueError as e:
                                    print(f"Error parsing FinalWindowProcessFunction timestamp in {filename}: '{last_token}' - {e}")

                            # Extract the aggregate value without regex.
                            parts = line.split("Emitted final aggregate value:")
                            if len(parts) > 1:
                                # Take the first token after the split, which should be the aggregate value.
                                agg_tokens = parts[1].strip().split()
                                if agg_tokens:
                                    agg_value_str = agg_tokens[0]
                                    try:
                                        agg_value = int(agg_value_str)
                                    except ValueError:
                                        try:
                                            agg_value = float(agg_value_str)
                                        except ValueError:
                                            agg_value = 0
                                    stats[filename]["fwp_product_sum"] += agg_value

                        # Process complex events (only count them).
                        if line.lstrip().startswith("complex"):
                            stats[filename]["complex_event_count"] += 1
            except FileNotFoundError:
                print(f"File {filename} not found.")

        if earliest is None:
            print("No 'was received at:' timestamp found.")
            sys.exit(1)
        if latest is None:
            print("No 'FinalWindowProcessFunction' timestamp found.")
            sys.exit(1)

        duration = latest - earliest

        print("Overall Timestamps:")
        print("  Earliest timestamp:", earliest, "from file:", earliest_file)
        print("  Latest timestamp:  ", latest, "from file:", latest_file)
        print("  Duration:          ", duration)
        print("\nPer File Statistics:")

        total_fwp_product_sum = 0
        for fname in sorted(stats.keys(), key=lambda x: int(x.split('.')[0])):
            data = stats[fname]
            total_fwp_product_sum += data["fwp_product_sum"]
            print(f"File: {fname}")
            print(f"  Messages received count:                   {data['msg_received_count']}")
            print(f"  Complex events count:                      {data['complex_event_count']}")
            print(f"  Sum from FinalWindowProcessFunction lines: {data['fwp_product_sum']}")
            print()

        # Call external command.
        try:
            cmd = "python stream_producer.py | ./sequence_accumulator_fat --type A ABCD"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                print("Error running external command:")
                print(result.stderr)
                sys.exit(1)
            external_output = result.stdout.strip()
            tokens = external_output.split()
            if len(tokens) % 4 != 0:
                print("External command output does not consist of complete tuples.")
                sys.exit(1)
            external_third_sum = 0
            for i in range(0, len(tokens), 4):
                try:
                    value = int(tokens[i+2])
                except ValueError:
                    value = 0
                external_third_sum += value
        except Exception as e:
            print("Exception while calling external command:", e)
            sys.exit(1)

        final_result = external_third_sum - total_fwp_product_sum
        print("Ground truth #matches:", external_third_sum)
        print("Computed #matches:", total_fwp_product_sum)
        if final_result == 0:
            print("Difference:", final_result, "- that's perfect!!")
        else:
            print("Difference:", final_result, "- almost.. :)")
    
    sys.stdout = original_stdout

if __name__ == "__main__":
    main()
