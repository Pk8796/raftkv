import argparse
import logging
import sys

from .config import load
from .node import Node
from .server import serve


def main():
    ap = argparse.ArgumentParser(prog="kvstore")
    ap.add_argument("--config", required=True)
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    node = Node(load(args.config))
    node.start()
    try:
        serve(node)
    except KeyboardInterrupt:
        node.stop()
        sys.exit(0)


if __name__ == "__main__":
    main()
