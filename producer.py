import json
import time
import argparse
from event_generator import generate_event


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval", type=float, default=0.0)
    args = parser.parse_args()

    for sequence_no in range(1, args.count + 1):
        event = generate_event(sequence_no)
        print(json.dumps(event), flush=True)
        if args.interval > 0:
            time.sleep(args.interval)

if __name__ == "__main__":
    main()
