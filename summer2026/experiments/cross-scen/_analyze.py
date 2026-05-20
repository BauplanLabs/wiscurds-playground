import json, pathlib, math

BASE = pathlib.Path("experiments/genx/runs")
scenarios = ["can_steady_bal", "prio_steady_bal", "can_bursty_cpu", "batch_bursty_mem"]
arms = {"baseline": 1, "simple": 5, "full": 5}
PENALTY = 720.0


def penalized_gm(sr):
    lats = []
    for s in sr:
        v = sr[s]
        if not isinstance(v, dict):
            continue
        lat = v["latency"] if v.get("ok") else PENALTY
        lats.append(lat)
    if not lats:
        return None
    return math.exp(sum(math.log(max(l, 1e-9)) for l in lats) / len(lats))


data = {arm: [] for arm in arms}
for scn in scenarios:
    for arm, k in arms.items():
        for i in range(1, k + 1):
            p = BASE / scn / arm / f"c{i}" / "eval_out.json"
            if not p.exists():
                continue
            d = json.loads(p.read_text(encoding="utf-8"))
            sr = d.get("scale_results", {})
            oks = [sr[s]["ok"] for s in sr if isinstance(sr[s], dict)]
            ok_all = all(oks) and len(oks) == 5
            gm = penalized_gm(sr)
            data[arm].append((scn, i, ok_all, gm))

print("=== per-draw (单次抽样) ===")
for arm in ["baseline", "simple", "full"]:
    draws = data[arm]
    total = len(draws)
    ok5 = sum(1 for x in draws if x[2])
    gms_all = sorted(x[3] for x in draws if x[3] is not None)
    gms_ok = sorted(x[3] for x in draws if x[2] and x[3] is not None)
    med_all = gms_all[len(gms_all) // 2] if gms_all else 0
    med_ok = gms_ok[len(gms_ok) // 2] if gms_ok else 0
    print(
        f"  {arm:8s}: n={total}  5/5通过={ok5}/{total}({100*ok5//total}%)"
        f"  penGM中位(含失败)={med_all:.1f}"
        f"  penGM中位(仅通过)={med_ok:.1f}"
    )

print()
print("=== best-of-N winner (每个 scenario 选最佳) ===")
for arm in ["baseline", "simple", "full"]:
    k = arms[arm]
    winners = []
    for scn in scenarios:
        best_gm = 1e9
        best_ok = None
        for i in range(1, k + 1):
            p = BASE / scn / arm / f"c{i}" / "eval_out.json"
            if not p.exists():
                continue
            d = json.loads(p.read_text(encoding="utf-8"))
            sr = d.get("scale_results", {})
            oks = [sr[s]["ok"] for s in sr if isinstance(sr[s], dict)]
            gm = penalized_gm(sr)
            if gm is not None and gm < best_gm:
                best_gm = gm
                best_ok = all(oks) and len(oks) == 5
        winners.append((scn, best_ok, best_gm))
    ok5 = sum(1 for x in winners if x[1])
    gms = [round(x[2], 1) for x in winners]
    avg = round(sum(x[2] for x in winners) / len(winners), 1)
    print(f"  {arm:8s}: winners_5/5={ok5}/4  penGMs={gms}  avg={avg}")

print()
print("=== 逐 scenario 明细 (winner penGM by arm) ===")
scn_short = {
    "can_steady_bal": "can_ste",
    "prio_steady_bal": "prio_ste",
    "can_bursty_cpu": "can_bur",
    "batch_bursty_mem": "bat_mem",
}
print(f"  {'scenario':12s}  {'baseline':>10s}  {'simple':>10s}  {'full':>10s}")
for scn in scenarios:
    row = []
    for arm, k in arms.items():
        best_gm = 1e9
        for i in range(1, k + 1):
            p = BASE / scn / arm / f"c{i}" / "eval_out.json"
            if not p.exists():
                continue
            d = json.loads(p.read_text(encoding="utf-8"))
            sr = d.get("scale_results", {})
            gm = penalized_gm(sr)
            if gm is not None and gm < best_gm:
                best_gm = gm
        row.append(best_gm)
    print(f"  {scn_short[scn]:12s}  {row[0]:>10.1f}  {row[1]:>10.1f}  {row[2]:>10.1f}")
