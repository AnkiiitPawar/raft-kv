"""Plot docs/timeline.csv (from `go run ./cmd/demo -mode timeline`). Needs matplotlib."""

import csv

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

rows, events = [], []
with open("docs/timeline.csv") as fh:
    for line in fh:
        if line.startswith("#"):
            t, desc = line[2:].strip().split(",", 1)
            events.append((float(t), desc))
        elif not line.startswith("t,"):
            t, node, term, role, alive, conn, ops = line.strip().split(",")
            rows.append((float(t), int(node), int(term), role, int(alive), int(conn), int(ops)))

fig, (a, b) = plt.subplots(2, 1, figsize=(12, 5.6), sharex=True, gridspec_kw={"height_ratios": [2.2, 1]})
colors = {"leader": "#c0504d", "follower": "#9fb7d0", "down": "#222222"}
# One bar per stretch of unchanged state, so irregular sampling leaves no gaps.
end_t = max(r[0] for r in rows)
for node in range(5):
    seq = [r for r in rows if r[1] == node]
    start = 0
    for k in range(1, len(seq) + 1):
        if k == len(seq) or (seq[k][3], seq[k][5]) != (seq[start][3], seq[start][5]):
            t0 = seq[start][0]
            t1 = seq[k][0] if k < len(seq) else end_t
            a.barh(node, t1 - t0, left=t0, color=colors[seq[start][3]], alpha=1.0 if seq[start][5] else 0.35,
                   height=0.7, linewidth=0)
            start = k
last = {}
for t, node, term, role, alive, conn, ops in rows:  # label each leadership with its term
    if role == "leader" and last.get(node) != term:
        a.text(t + 0.05, node, f"term {term}", va="center", fontsize=7, color="white", fontweight="bold")
    last[node] = term if role == "leader" else None
short = {"isolate the leader": "leader cut off", "reconnect everyone": "all reconnected",
         "isolate the leader and one follower": "leader + follower cut off",
         "isolate a third server: no majority left": "a third cut off: no majority",
         "crash the leader": "leader crashes", "restart it from disk": "restarted from disk"}
for k, (t, desc) in enumerate(events):
    for ax in (a, b):
        ax.axvline(t, color="#555", lw=0.6, ls="--")
    a.text(t + 0.04, 4.5 + 0.38 * (k % 2), short.get(desc, desc), fontsize=7.5, va="bottom")
a.set(yticks=range(5), yticklabels=[f"server {i}" for i in range(5)], ylim=(-0.6, 5.5))
a.set_title("Leader (red), follower (blue), crashed (black); faded = cut off from the network", fontsize=9)
ts = sorted({r[0] for r in rows})
ops = {r[0]: r[6] for r in rows}
b.plot(ts, [ops[t] for t in ts], color="#1f5f99")
b.set(xlabel="time (s)", ylabel="appends committed")
for ax in (a, b):
    ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout()
fig.savefig("docs/timeline.png", dpi=120)
print("wrote docs/timeline.png")
