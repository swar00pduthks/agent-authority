"""`authority` command line.

    authority demo                 # in-memory hub + live synthetic traffic + web UI
    authority serve                # API (+UI) using AUTHORITY_STORE=memory|prod
    authority collect              # Kafka -> collector -> stores (production ingest)
    authority seed --url URL       # push demo traffic to a running hub (or Marquez)
    authority scan                 # run Sentinel against a hub and print findings
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import urllib.request
import webbrowser

BANNER = """
   (o)  Agent Authority
   |    every agent action, traced to the person who authorised it
   o
"""


def cmd_demo(args: argparse.Namespace) -> None:
    import uvicorn

    from agent_authority.api.main import create_app
    from agent_authority.demo import DemoTraffic
    from agent_authority.storage.memory import MemoryStore

    app = create_app(MemoryStore())
    traffic = DemoTraffic(app.state.ingest, rate=args.rate)
    traffic.seed(args.seed)
    traffic.start()
    url = f"http://localhost:{args.port}"
    print(BANNER)
    s = app.state.store.stats()
    print(f"  seeded {s['events_in']} OpenLineage events -> {s['runs']} runs, {s['nodes']} nodes, "
          f"{s['dedup_ratio']}x smaller after dehydration")
    print(f"  UI      {url}")
    print(f"  API     {url}/docs")
    print(f"  ingest  POST {url}/api/v1/lineage  (point any OpenLineage emitter here)\n")
    if not args.no_browser:
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


def cmd_serve(args: argparse.Namespace) -> None:
    os.environ.setdefault("PORT", str(args.port))
    from agent_authority.api.main import run

    run()


def cmd_collect(args: argparse.Namespace) -> None:
    from agent_authority.collector.kafka_consumer import main

    main()


def cmd_seed(args: argparse.Namespace) -> None:
    from agent_authority.demo import DemoTraffic

    endpoint = args.url.rstrip("/") + "/api/v1/lineage"

    def post(events: list[dict]) -> None:
        for ev in events:  # one per request: Marquez only accepts single events
            req = urllib.request.Request(endpoint, json.dumps(ev).encode(), {"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10).close()

    sink = post
    if args.kafka:
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": args.kafka})
        endpoint = f"kafka://{args.kafka}/{args.topic}"

        def to_kafka(events: list[dict]) -> None:
            for ev in events:  # key by run id so a run's events stay ordered within a partition
                producer.produce(args.topic, json.dumps(ev).encode(), key=ev["run"]["runId"].encode())
            producer.poll(0)

        sink = to_kafka

    t = DemoTraffic(sink)
    t.seed(args.count)
    if args.kafka:
        producer.flush(10)
    print(f"seeded {endpoint}")
    if args.follow:
        t.run_forever()


def cmd_scan(args: argparse.Namespace) -> None:
    url = args.url.rstrip("/") + "/api/authority/findings" + (f"?severity={args.severity}" if args.severity else "")
    findings = json.load(urllib.request.urlopen(url, timeout=30))
    colors = {"critical": "\033[1;31m", "high": "\033[31m", "medium": "\033[33m", "low": "\033[36m", "info": "\033[2m"}
    for f in findings:
        c = colors.get(f["severity"], "")
        print(f"{c}{f['severity'].upper():9}\033[0m {f['rule']:26} {f['title']}")
    print(f"\n{len(findings)} finding(s)")
    if args.fail_on and any(f["severity"] in ("critical", "high") for f in findings):
        sys.exit(2)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "WARNING"))
    p = argparse.ArgumentParser(prog="authority",
                                description="Agent Authority: lineage and accountability for data and AI agents")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("demo", help="run an in-memory hub with live demo traffic and the web UI")
    d.add_argument("--port", type=int, default=8000)
    d.add_argument("--host", default="127.0.0.1")
    d.add_argument("--rate", type=float, default=1.5, help="demo scenarios per second")
    d.add_argument("--seed", type=int, default=80, help="scenarios to pre-seed")
    d.add_argument("--no-browser", action="store_true")
    d.set_defaults(fn=cmd_demo)

    s = sub.add_parser("serve", help="run the API server")
    s.add_argument("--port", type=int, default=8000)
    s.set_defaults(fn=cmd_serve)

    c = sub.add_parser("collect", help="run the Kafka collector")
    c.set_defaults(fn=cmd_collect)

    sd = sub.add_parser("seed", help="send demo traffic to a running hub or any OpenLineage endpoint")
    sd.add_argument("--url", default="http://localhost:8000")
    sd.add_argument("--count", type=int, default=50)
    sd.add_argument("--follow", action="store_true", help="keep sending live traffic")
    sd.add_argument("--kafka", metavar="BOOTSTRAP", help="produce to Kafka instead of HTTP, e.g. localhost:29092")
    sd.add_argument("--topic", default="openlineage.events")
    sd.set_defaults(fn=cmd_seed)

    sc = sub.add_parser("scan", help="print Sentinel findings (use --fail-on in CI)")
    sc.add_argument("--url", default="http://localhost:8000")
    sc.add_argument("--severity")
    sc.add_argument("--fail-on", action="store_true", help="exit 2 if any critical/high findings")
    sc.set_defaults(fn=cmd_scan)

    args = p.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
