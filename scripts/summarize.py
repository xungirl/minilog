#!/usr/bin/env python3
"""Summarise results/*.txt RESULT lines into markdown tables (median and min-max across reps)."""
import re, statistics, sys, pathlib
from collections import defaultdict

root = pathlib.Path(__file__).resolve().parent.parent / "results"

def parse(name):
    rows = []
    f = root / name
    if not f.exists():
        return rows
    for line in f.read_text().splitlines():
        if line.startswith("RESULT"):
            kv = dict(re.findall(r"(\w+)=(\S+)", line))
            rows.append(kv)
    return rows

def rng(vals, fmt="{:,.0f}"):
    vals = sorted(vals)
    return f"{fmt.format(statistics.median(vals))} ({fmt.format(vals[0])} – {fmt.format(vals[-1])}, n={len(vals)})"

def throughput_table(name, keys):
    rows = parse(name)
    groups = defaultdict(list)
    for r in rows:
        groups[tuple(r[k] for k in keys)].append(r)
    print(f"\n### {name}\n")
    print("| " + " | ".join(keys) + " | msgs/s median (min – max) | MiB/s median (min – max) |")
    print("|" + "---|" * (len(keys) + 2))
    for k in sorted(groups, key=lambda t: tuple(float(x) if x.lstrip('-').replace('.','').isdigit() else x for x in t)):
        g = groups[k]
        print("| " + " | ".join(k) + " | " + rng([float(r["msgs_per_sec"]) for r in g]) + " | "
              + rng([float(r["mib_per_sec"]) for r in g], "{:,.1f}") + " |")

throughput_table("produce.txt", ["size", "acks", "batch"])
throughput_table("consume.txt", ["size"])
throughput_table("produce-long.txt", ["size", "acks", "batch"])
throughput_table("consume-long.txt", ["size"])
throughput_table("fsync.txt", ["policy", "acks"])

rows = parse("e2e.txt")
groups = defaultdict(list)
for r in rows:
    groups[r["acks"]].append(r)
print("\n### e2e.txt\n\n| acks | p50 ms | p99 ms | p99.9 ms | max ms |\n|---|---|---|---|---|")
for a, g in sorted(groups.items()):
    print(f"| {a} | " + " | ".join(rng([float(r[k]) for r in g], "{:.3f}") for k in ["p50_ms", "p99_ms", "p999_ms", "max_ms"]) + " |")

print("\n### failover.txt\n")
for r in parse("failover.txt"):
    print(r)
print("\n### chaos.txt\n")
for r in parse("chaos.txt"):
    print(r)
