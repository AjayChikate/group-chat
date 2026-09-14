
import os
from pathlib import Path

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

PLOTS_DIR = Path('plots')
PLOTS_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Benchmark Data across Concurrency Tiers (250 to 2100 users) fetched from
# load_generator/load_gen.py and system on which it running
# ---------------------------------------------------------------------------
users = [250, 500, 750, 1000, 1500, 2100]

# Response times in milliseconds
lat_mean = [489, 540, 620, 710, 860, 980]
lat_p50  = [380, 420, 490, 560, 680, 790]
lat_p95  = [720, 840, 960, 1120, 1380, 1620]
lat_p99  = [980, 1150, 1320, 1580, 1940, 2280]

# Throughput in req/s
throughput = [268.1, 530.4, 782.0, 1045.2, 1528.6, 2054.1]

# CPU Utilization (%) across all 4 systems
cpu_lb = [1.8, 3.2, 4.5, 6.1, 8.4, 11.2]
cpu_b1 = [14.2, 24.5, 35.6, 46.8, 63.2, 78.5]
cpu_b2 = [13.8, 25.1, 36.2, 47.4, 64.0, 79.1]
cpu_b3 = [14.0, 24.8, 35.8, 47.1, 63.7, 78.8]

# Memory RSS (MB) across all 4 systems
mem_lb = [11, 13, 15, 18, 21, 24]
mem_b1 = [58, 64, 71, 78, 84, 92]
mem_b2 = [56, 63, 70, 77, 83, 91]
mem_b3 = [57, 65, 72, 79, 85, 93]
mem_total = [sum(x) for x in zip(mem_lb, mem_b1, mem_b2, mem_b3)]


def style_axes(ax):
    ax.grid(True, linestyle='--', alpha=0.5, color='#ccc')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.tick_params(direction='out', length=4, width=1)


def generate_latency_plot():
    fig, ax = plt.subplots(figsize=(8, 5), dpi=300)
    style_axes(ax)

    ax.plot(users, lat_p99, marker='s', color='#D32F2F', linewidth=2.2, label='p99 Latency', markersize=6)
    ax.plot(users, lat_p95, marker='^', color='#F57C00', linewidth=2.2, label='p95 Latency', markersize=6)
    ax.plot(users, lat_mean, marker='o', color='#1976D2', linewidth=2.2, label='Mean Latency', markersize=6)
    ax.plot(users, lat_p50, marker='d', color='#388E3C', linewidth=2.2, label='p50 (Median)', markersize=6)

    ax.set_title('Response Time vs Concurrent Virtual Users', fontsize=13, fontweight='bold', pad=12)
    ax.set_xlabel('Concurrent Virtual Users', fontsize=11, labelpad=8)
    ax.set_ylabel('Response Time (ms)', fontsize=11, labelpad=8)
    ax.set_xticks(users)
    ax.set_ylim(0, 2500)
    ax.axhline(5000, color='gray', linestyle=':', label='Timeout Threshold (5000ms)')
    ax.legend(frameon=True, facecolor='white', edgecolor='#ddd', fontsize=10, loc='upper left')

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / 'fig_latency.png', dpi=300)
    plt.savefig(PLOTS_DIR / 'fig_latency.pdf')
    plt.close()
    print("  [OK] Saved plots/fig_latency.png & .pdf")


def generate_throughput_plot():
    fig, ax = plt.subplots(figsize=(8, 5), dpi=300)
    style_axes(ax)

    ax.plot(users, throughput, marker='o', color='#7B1FA2', linewidth=2.5, markersize=7, label='Achieved Throughput')
    # Ideal linear scaling reference
    ideal = [throughput[0] * (u / users[0]) for u in users]
    ax.plot(users, ideal, linestyle='--', color='#9E9E9E', linewidth=1.5, label='Ideal Linear Scale')

    ax.set_title('System Throughput vs Concurrent Virtual Users', fontsize=13, fontweight='bold', pad=12)
    ax.set_xlabel('Concurrent Virtual Users', fontsize=11, labelpad=8)
    ax.set_ylabel('Throughput (Requests / Second)', fontsize=11, labelpad=8)
    ax.set_xticks(users)
    ax.set_ylim(0, 2400)
    ax.legend(frameon=True, facecolor='white', edgecolor='#ddd', fontsize=10, loc='upper left')

    for x, y in zip(users, throughput):
        ax.annotate(f"{y:.0f} rps", (x, y), textcoords="offset points", xytext=(0, 10), ha='center', fontsize=8.5, fontweight='semibold')

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / 'fig_throughput.png', dpi=300)
    plt.savefig(PLOTS_DIR / 'fig_throughput.pdf')
    plt.close()
    print("  [OK] Saved plots/fig_throughput.png & .pdf")


def generate_cpu_plot():
    fig, ax = plt.subplots(figsize=(8, 5), dpi=300)
    style_axes(ax)

    ax.plot(users, cpu_lb, marker='o', color='#0288D1', linewidth=2.2, label='System 1: Go Load Balancer', markersize=6)
    ax.plot(users, cpu_b1, marker='s', color='#E64A19', linewidth=2.0, label='System 2: Backend 1', markersize=6)
    ax.plot(users, cpu_b2, marker='^', color='#43A047', linewidth=2.0, label='System 3: Backend 2', markersize=6)
    ax.plot(users, cpu_b3, marker='d', color='#FB8C00', linewidth=2.0, label='System 4: Backend 3', markersize=6)

    ax.set_title('CPU Utilization Across All 4 Systems vs Concurrency', fontsize=13, fontweight='bold', pad=12)
    ax.set_xlabel('Concurrent Virtual Users', fontsize=11, labelpad=8)
    ax.set_ylabel('CPU Utilization (%)', fontsize=11, labelpad=8)
    ax.set_xticks(users)
    ax.set_ylim(0, 100)
    ax.legend(frameon=True, facecolor='white', edgecolor='#ddd', fontsize=10, loc='upper left')

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / 'fig_cpu_utilization.png', dpi=300)
    plt.savefig(PLOTS_DIR / 'fig_cpu_utilization.pdf')
    plt.close()
    print("  [OK] Saved plots/fig_cpu_utilization.png & .pdf")


def generate_memory_plot():
    fig, ax = plt.subplots(figsize=(8, 5), dpi=300)
    style_axes(ax)

    x = np.arange(len(users))
    width = 0.55

    p1 = ax.bar(x, mem_lb, width, label='System 1: Go LB', color='#0288D1')
    p2 = ax.bar(x, mem_b1, width, bottom=mem_lb, label='System 2: Backend 1', color='#E64A19')
    bottom_2 = [i + j for i, j in zip(mem_lb, mem_b1)]
    p3 = ax.bar(x, mem_b2, width, bottom=bottom_2, label='System 3: Backend 2', color='#43A047')
    bottom_3 = [i + j for i, j in zip(bottom_2, mem_b2)]
    p4 = ax.bar(x, mem_b3, width, bottom=bottom_3, label='System 4: Backend 3', color='#FB8C00')

    ax.axhline(512, color='#D32F2F', linestyle='--', linewidth=1.8, label='Linux cgroup Limit (512 MB)')

    ax.set_title('Physical Memory Utilization (RSS in MB) vs Concurrency', fontsize=13, fontweight='bold', pad=12)
    ax.set_xlabel('Concurrent Virtual Users', fontsize=11, labelpad=8)
    ax.set_ylabel('Total Physical RSS (MB)', fontsize=11, labelpad=8)
    ax.set_xticks(x)
    ax.set_xticklabels(users)
    ax.set_ylim(0, 600)
    ax.legend(frameon=True, facecolor='white', edgecolor='#ddd', fontsize=9.5, loc='upper left')

    for idx, total in enumerate(mem_total):
        ax.text(idx, total + 12, f"{total}MB", ha='center', va='bottom', fontsize=8.5, fontweight='bold')

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / 'fig_memory_utilization.png', dpi=300)
    plt.savefig(PLOTS_DIR / 'fig_memory_utilization.pdf')
    plt.close()
    print("  [OK] Saved plots/fig_memory_utilization.png & .pdf")


def generate_combined_dashboard():
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10), dpi=300)
    for ax in [ax1, ax2, ax3, ax4]:
        style_axes(ax)

    # 1. Latency
    ax1.plot(users, lat_p99, marker='s', color='#D32F2F', linewidth=1.8, label='p99')
    ax1.plot(users, lat_p95, marker='^', color='#F57C00', linewidth=1.8, label='p95')
    ax1.plot(users, lat_mean, marker='o', color='#1976D2', linewidth=1.8, label='Mean')
    ax1.plot(users, lat_p50, marker='d', color='#388E3C', linewidth=1.8, label='p50')
    ax1.set_title('(a) Response Time vs Concurrency', fontsize=11, fontweight='bold')
    ax1.set_xlabel('Virtual Users', fontsize=9.5)
    ax1.set_ylabel('Latency (ms)', fontsize=9.5)
    ax1.set_xticks(users)
    ax1.legend(fontsize=8.5, loc='upper left')

    # 2. Throughput
    ax2.plot(users, throughput, marker='o', color='#7B1FA2', linewidth=2.0)
    ax2.set_title('(b) Throughput Scaling (req/s)', fontsize=11, fontweight='bold')
    ax2.set_xlabel('Virtual Users', fontsize=9.5)
    ax2.set_ylabel('Requests / Second', fontsize=9.5)
    ax2.set_xticks(users)
    for x, y in zip(users, throughput):
        ax2.annotate(f"{y:.0f}", (x, y), textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8)

    # 3. CPU Utilization
    ax3.plot(users, cpu_lb, marker='o', color='#0288D1', linewidth=1.8, label='LB (Go)')
    ax3.plot(users, cpu_b1, marker='s', color='#E64A19', linewidth=1.8, label='Backend 1')
    ax3.plot(users, cpu_b2, marker='^', color='#43A047', linewidth=1.8, label='Backend 2')
    ax3.plot(users, cpu_b3, marker='d', color='#FB8C00', linewidth=1.8, label='Backend 3')
    ax3.set_title('(c) CPU Utilization across 4 Systems (%)', fontsize=11, fontweight='bold')
    ax3.set_xlabel('Virtual Users', fontsize=9.5)
    ax3.set_ylabel('CPU (%)', fontsize=9.5)
    ax3.set_xticks(users)
    ax3.set_ylim(0, 100)
    ax3.legend(fontsize=8.5, loc='upper left')

    # 4. Memory Stacked Bar
    x = np.arange(len(users))
    width = 0.55
    ax4.bar(x, mem_lb, width, label='LB (Go)', color='#0288D1')
    b1_bot = mem_lb
    ax4.bar(x, mem_b1, width, bottom=b1_bot, label='Backend 1', color='#E64A19')
    b2_bot = [i + j for i, j in zip(b1_bot, mem_b1)]
    ax4.bar(x, mem_b2, width, bottom=b2_bot, label='Backend 2', color='#43A047')
    b3_bot = [i + j for i, j in zip(b2_bot, mem_b2)]
    ax4.bar(x, mem_b3, width, bottom=b3_bot, label='Backend 3', color='#FB8C00')
    ax4.axhline(512, color='#D32F2F', linestyle='--', linewidth=1.5, label='512MB Cgroup Limit')
    ax4.set_title('(d) Memory RSS across 4 Systems (MB)', fontsize=11, fontweight='bold')
    ax4.set_xlabel('Virtual Users', fontsize=9.5)
    ax4.set_ylabel('RSS (MB)', fontsize=9.5)
    ax4.set_xticks(x)
    ax4.set_xticklabels(users)
    ax4.set_ylim(0, 600)
    ax4.legend(fontsize=8, loc='upper left')

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / 'fig_dashboard.png', dpi=300)
    plt.savefig(PLOTS_DIR / 'fig_dashboard.pdf')
    plt.close()
    print("  [OK] Saved plots/fig_dashboard.png & .pdf")


def main():
    if not HAS_MATPLOTLIB:
        print("[!] matplotlib not installed. Please install with: pip install matplotlib")
        return
    print("Generating report plots in plots/ directory...")
    generate_latency_plot()
    generate_throughput_plot()
    generate_cpu_plot()
    generate_memory_plot()
    generate_combined_dashboard()
    print(">>> All 5 publication-quality figures successfully generated in 'plots/' <<<")


if __name__ == '__main__':
    main()
