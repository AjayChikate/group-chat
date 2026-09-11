#!/usr/bin/env python3
"""
load_gen.py — Async Load Generator for Group Chat API
=====================================================

Usage examples:
  # 200 concurrent users, 60 seconds
  python load_gen.py --users 200 --duration 60 --lb-url http://LB_IP:8080

  # 1000 users, variable message sizes, ramp-up over 30 s
  python load_gen.py --users 1000 --duration 120 --lb-url http://LB_IP:8080 \\
    --msg-min-len 10 --msg-max-len 500 --interval-min 0.1 --interval-max 2.0 \\
    --ramp-up 30 --plot

  # Scale test: 200 → 500 → 1000 → 2000 users
  python load_gen.py --scale-test --lb-url http://LB_IP:8080 --plot

Output:
  • Console: live throughput + error rate every 5 s
  • CSV:  results/results_<timestamp>.csv
  • Plot: results/plot_<timestamp>.png  (if --plot)
"""

import argparse
import asyncio
import csv
import os
import random
import string
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import aiohttp

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# ---------------------------------------------------------------------------
# Constants / helpers
# ---------------------------------------------------------------------------

RESULTS_DIR = Path('results')
RESULTS_DIR.mkdir(exist_ok=True)

ADJECTIVES = ['quick', 'lazy', 'blue', 'happy', 'sad', 'fast', 'slow', 'bright', 'dark']
NOUNS = ['fox', 'dog', 'cat', 'bird', 'tree', 'cloud', 'river', 'stone', 'wind']
WORDS = ADJECTIVES + NOUNS + [
    'hello', 'world', 'test', 'message', 'chat', 'group', 'secure',
    'python', 'golang', 'server', 'load', 'balance', 'scale', 'data',
]


def random_message(min_len: int, max_len: int) -> str:
    target = random.randint(min_len, max_len)
    words = []
    total = 0
    while total < target:
        w = random.choice(WORDS)
        words.append(w)
        total += len(w) + 1
    return ' '.join(words)[:max_len]


def random_username(user_id: int) -> str:
    return f"user_{user_id}_{random.randint(100,999)}"


# ---------------------------------------------------------------------------
# Stats collector (thread-safe via asyncio — single event loop)
# ---------------------------------------------------------------------------

class Stats:
    def __init__(self):
        self.latencies: List[float] = []  # ms
        self.errors: int = 0
        self.success: int = 0
        self.window: deque = deque(maxlen=1000)  # recent latencies for live display
        self.timeseries: List[Tuple[float, float, int, int]] = []  # (t, tput, err, ok)
        self._lock = asyncio.Lock()

    async def record(self, latency_ms: float, ok: bool):
        async with self._lock:
            if ok:
                self.success += 1
                self.latencies.append(latency_ms)
                self.window.append(latency_ms)
            else:
                self.errors += 1

    def snapshot(self, elapsed: float) -> dict:
        total = self.success + self.errors
        tput = total / elapsed if elapsed > 0 else 0
        lats = list(self.window)
        p50 = p95 = p99 = 0.0
        if lats:
            sl = sorted(lats)
            p50 = sl[int(len(sl) * 0.50)]
            p95 = sl[int(len(sl) * 0.95)]
            p99 = sl[int(len(sl) * 0.99)]
        err_rate = self.errors / total * 100 if total > 0 else 0
        return {
            'elapsed': elapsed,
            'total': total,
            'success': self.success,
            'errors': self.errors,
            'tput_rps': tput,
            'err_rate_pct': err_rate,
            'p50_ms': p50,
            'p95_ms': p95,
            'p99_ms': p99,
        }


# ---------------------------------------------------------------------------
# Backend metrics scraper (polls /metrics on the LB)
# ---------------------------------------------------------------------------

class MetricsScraper:
    def __init__(self, lb_url: str, interval: float = 5.0):
        self.lb_url = lb_url.rstrip('/')
        self.interval = interval
        self.history: List[dict] = []
        self._running = False

    async def run(self, session: aiohttp.ClientSession):
        self._running = True
        while self._running:
            try:
                async with session.get(f'{self.lb_url}/metrics', timeout=aiohttp.ClientTimeout(total=3)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        data['_t'] = time.time()
                        self.history.append(data)
            except Exception:
                pass
            await asyncio.sleep(self.interval)

    def stop(self):
        self._running = False


# ---------------------------------------------------------------------------
# Virtual user coroutine
# ---------------------------------------------------------------------------

async def virtual_user(
    user_id: int,
    session: aiohttp.ClientSession,
    lb_url: str,
    stats: Stats,
    duration: float,
    start_time: float,
    msg_min_len: int,
    msg_max_len: int,
    interval_min: float,
    interval_max: float,
    ramp_delay: float = 0.0,
):
    if ramp_delay > 0:
        await asyncio.sleep(ramp_delay)

    username = random_username(user_id)
    end_time = start_time + duration

    while time.time() < end_time:
        msg = random_message(msg_min_len, msg_max_len)
        payload = {'client-name': username, 'msg': msg}

        t0 = time.time()
        ok = False
        try:
            async with session.post(
                f'{lb_url}/message',
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                ok = resp.status < 500
                await resp.read()  # consume body
        except Exception:
            ok = False

        latency_ms = (time.time() - t0) * 1000
        await stats.record(latency_ms, ok)

        # Random interval between requests
        sleep_s = random.uniform(interval_min, interval_max)
        await asyncio.sleep(sleep_s)


# ---------------------------------------------------------------------------
# Reporter coroutine
# ---------------------------------------------------------------------------

async def reporter(stats: Stats, start_time: float, total_duration: float):
    interval = 5.0
    while True:
        await asyncio.sleep(interval)
        elapsed = time.time() - start_time
        if elapsed > total_duration + interval:
            break
        s = stats.snapshot(elapsed)
        # Record timeseries
        stats.timeseries.append((elapsed, s['tput_rps'], s['errors'], s['success']))
        print(
            f"  [{elapsed:6.1f}s] "
            f"tput={s['tput_rps']:6.1f} rps | "
            f"p50={s['p50_ms']:6.1f}ms | "
            f"p95={s['p95_ms']:6.1f}ms | "
            f"p99={s['p99_ms']:6.1f}ms | "
            f"err={s['err_rate_pct']:5.1f}%"
        )


# ---------------------------------------------------------------------------
# Single run (fixed user count)
# ---------------------------------------------------------------------------

async def run_load_test(
    lb_url: str,
    num_users: int,
    duration: float,
    msg_min_len: int,
    msg_max_len: int,
    interval_min: float,
    interval_max: float,
    ramp_up: float = 0.0,
) -> Tuple[Stats, MetricsScraper]:

    stats = Stats()
    scraper = MetricsScraper(lb_url)
    start = time.time()

    connector = aiohttp.TCPConnector(
        limit=num_users + 50,
        limit_per_host=num_users + 50,
        ttl_dns_cache=300,
    )

    print(f"\n{'='*60}")
    print(f"  Load test: {num_users} users | {duration}s | {lb_url}")
    print(f"  Message length: {msg_min_len}–{msg_max_len} chars")
    print(f"  Interval: {interval_min}–{interval_max}s")
    if ramp_up > 0:
        print(f"  Ramp-up: {ramp_up}s")
    print(f"{'='*60}\n")

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []

        # Metrics scraper task
        tasks.append(asyncio.create_task(scraper.run(session)))

        # Reporter task
        tasks.append(asyncio.create_task(reporter(stats, start, duration)))

        # Virtual user tasks
        for uid in range(num_users):
            ramp_delay = (uid / num_users) * ramp_up if ramp_up > 0 else 0
            tasks.append(asyncio.create_task(
                virtual_user(
                    uid, session, lb_url, stats,
                    duration, start,
                    msg_min_len, msg_max_len,
                    interval_min, interval_max,
                    ramp_delay,
                )
            ))

        # Wait for all users to finish
        user_tasks = tasks[2:]  # skip scraper + reporter
        await asyncio.gather(*user_tasks, return_exceptions=True)

        scraper.stop()
        for t in tasks[:2]:
            t.cancel()
        await asyncio.gather(*tasks[:2], return_exceptions=True)

    elapsed = time.time() - start
    s = stats.snapshot(elapsed)

    print(f"\n{'='*60}")
    print(f"  RESULTS — {num_users} users")
    print(f"{'='*60}")
    print(f"  Duration       : {elapsed:.1f} s")
    print(f"  Total requests : {s['total']}")
    print(f"  Success        : {s['success']}")
    print(f"  Errors         : {s['errors']} ({s['err_rate_pct']:.1f}%)")
    print(f"  Throughput     : {s['tput_rps']:.1f} req/s")
    print(f"  Latency p50    : {s['p50_ms']:.1f} ms")
    print(f"  Latency p95    : {s['p95_ms']:.1f} ms")
    print(f"  Latency p99    : {s['p99_ms']:.1f} ms")
    print(f"{'='*60}\n")

    return stats, scraper


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def save_csv(stats: Stats, scraper: MetricsScraper, label: str, timestamp: str):
    path = RESULTS_DIR / f'results_{label}_{timestamp}.csv'
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['latency_ms'])
        for lat in stats.latencies:
            w.writerow([f'{lat:.2f}'])
    print(f"  CSV saved: {path}")

    mpath = RESULTS_DIR / f'metrics_{label}_{timestamp}.csv'
    if scraper.history:
        keys = ['_t', 'lb_requests', 'lb_errors', 'uptime_sec']
        with open(mpath, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(keys)
            for entry in scraper.history:
                w.writerow([entry.get(k, '') for k in keys])
        print(f"  Metrics CSV saved: {mpath}")


# ---------------------------------------------------------------------------
# Plot generator
# ---------------------------------------------------------------------------

def plot_results(
    all_stats: List[Tuple[int, Stats, MetricsScraper]],
    timestamp: str,
):
    if not HAS_MATPLOTLIB:
        print("  [plot] matplotlib not installed — skipping plots")
        return

    fig = plt.figure(figsize=(16, 10))
    gs = gridspec.GridSpec(2, 2, figure=fig)

    ax_lat = fig.add_subplot(gs[0, 0])
    ax_tput = fig.add_subplot(gs[0, 1])
    ax_cpu = fig.add_subplot(gs[1, 0])
    ax_err = fig.add_subplot(gs[1, 1])

    colors = ['#2196F3', '#4CAF50', '#FF9800', '#E91E63']

    for idx, (users, stats, scraper) in enumerate(all_stats):
        color = colors[idx % len(colors)]
        label = f'{users} users'

        # Latency CDF
        if stats.latencies:
            sl = sorted(stats.latencies)
            cdf = [i / len(sl) for i in range(len(sl))]
            ax_lat.plot(sl, cdf, color=color, label=label, linewidth=2)

        # Throughput over time
        if stats.timeseries:
            ts_t = [x[0] for x in stats.timeseries]
            ts_tput = [x[1] for x in stats.timeseries]
            ax_tput.plot(ts_t, ts_tput, color=color, label=label, linewidth=2, marker='o', markersize=4)

        # CPU % per backend from scraper
        if scraper.history:
            times = [h['_t'] - scraper.history[0]['_t'] for h in scraper.history]
            for bi, backend_data in enumerate(scraper.history[0].get('backends', [])):
                b_label = f"B{bi+1} ({users}u)"
                cpu_vals = []
                for h in scraper.history:
                    backends = h.get('backends', [])
                    if bi < len(backends):
                        cpu_vals.append(backends[bi].get('cpu_pct', 0))
                    else:
                        cpu_vals.append(0)
                ax_cpu.plot(times, cpu_vals, label=b_label, linewidth=1.5, linestyle='--' if bi > 0 else '-')

        # Error rate over time
        if stats.timeseries:
            ts_t = [x[0] for x in stats.timeseries]
            ts_ok = [x[3] for x in stats.timeseries]
            ts_err = [x[2] for x in stats.timeseries]
            ts_total = [o + e for o, e in zip(ts_ok, ts_err)]
            ts_err_rate = [
                e / t * 100 if t > 0 else 0
                for e, t in zip(ts_err, ts_total)
            ]
            ax_err.plot(ts_t, ts_err_rate, color=color, label=label, linewidth=2)

    ax_lat.set_xlabel('Latency (ms)')
    ax_lat.set_ylabel('CDF')
    ax_lat.set_title('Response Time CDF')
    ax_lat.legend()
    ax_lat.grid(True, alpha=0.3)
    ax_lat.set_xlim(left=0)

    ax_tput.set_xlabel('Time (s)')
    ax_tput.set_ylabel('Throughput (req/s)')
    ax_tput.set_title('Throughput Over Time')
    ax_tput.legend()
    ax_tput.grid(True, alpha=0.3)

    ax_cpu.set_xlabel('Time (s)')
    ax_cpu.set_ylabel('CPU Utilization (%)')
    ax_cpu.set_title('Backend CPU Utilization')
    ax_cpu.legend(fontsize=8)
    ax_cpu.grid(True, alpha=0.3)
    ax_cpu.set_ylim(0, 100)

    ax_err.set_xlabel('Time (s)')
    ax_err.set_ylabel('Error Rate (%)')
    ax_err.set_title('Error Rate Over Time')
    ax_err.legend()
    ax_err.grid(True, alpha=0.3)
    ax_err.set_ylim(bottom=0)

    fig.suptitle(f'Group Chat Load Test Results — {timestamp}', fontsize=14, fontweight='bold')
    plt.tight_layout()

    plot_path = RESULTS_DIR / f'plot_{timestamp}.png'
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    print(f"  Plot saved: {plot_path}")
    plt.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description='Group Chat Load Generator')
    p.add_argument('--lb-url', default='http://localhost:8080', help='Load balancer base URL')
    p.add_argument('--users', type=int, default=100, help='Number of concurrent virtual users')
    p.add_argument('--duration', type=float, default=60.0, help='Test duration in seconds')
    p.add_argument('--msg-min-len', type=int, default=5, help='Minimum message length (chars)')
    p.add_argument('--msg-max-len', type=int, default=200, help='Maximum message length (chars)')
    p.add_argument('--interval-min', type=float, default=0.05, help='Min delay between messages (s)')
    p.add_argument('--interval-max', type=float, default=1.0, help='Max delay between messages (s)')
    p.add_argument('--ramp-up', type=float, default=0.0, help='Ramp-up period in seconds')
    p.add_argument('--plot', action='store_true', help='Generate plots (requires matplotlib)')
    p.add_argument('--scale-test', action='store_true',
                   help='Run scale test: 200 → 500 → 1000 → 2000 users')
    return p.parse_args()


async def main():
    args = parse_args()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    all_results = []

    if args.scale_test:
        stages = [200, 500, 1000, 2000]
        print(f"\n>>> Scale test: {stages} users @ {args.lb_url}")
        for users in stages:
            stats, scraper = await run_load_test(
                lb_url=args.lb_url,
                num_users=users,
                duration=args.duration,
                msg_min_len=args.msg_min_len,
                msg_max_len=args.msg_max_len,
                interval_min=args.interval_min,
                interval_max=args.interval_max,
                ramp_up=args.ramp_up,
            )
            save_csv(stats, scraper, str(users), timestamp)
            all_results.append((users, stats, scraper))
            # Brief pause between stages
            if users != stages[-1]:
                print("  Cooling down 10 s before next stage...")
                await asyncio.sleep(10)
    else:
        stats, scraper = await run_load_test(
            lb_url=args.lb_url,
            num_users=args.users,
            duration=args.duration,
            msg_min_len=args.msg_min_len,
            msg_max_len=args.msg_max_len,
            interval_min=args.interval_min,
            interval_max=args.interval_max,
            ramp_up=args.ramp_up,
        )
        save_csv(stats, scraper, str(args.users), timestamp)
        all_results.append((args.users, stats, scraper))

    if args.plot:
        plot_results(all_results, timestamp)
    else:
        print("  Tip: re-run with --plot to generate latency/throughput/CPU charts")


if __name__ == '__main__':
    asyncio.run(main())

