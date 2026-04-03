# trace_generator.py

## What it is

A standalone Python module that generates CSV workload trace files compatible with Eudoxia's replay format. The purpose is to create controlled, reproducible traces for scheduler experiments — where you can precisely dictate the workload mix, operator resource profiles, DAG structure, arrival timing, and the label columns that carry extra per-operator metadata for schedulers and estimators to use.

The standard Eudoxia trace has nine fixed columns. This generator writes those same nine columns, then appends any number of additional label columns after them. Eudoxia's reader treats those extra columns as the `labels` dict on each operator, so the traces are natively compatible with no changes to the simulator.

---

## Quick start

### CLI — run the built-in demo

```bash
# Writes demo_trace.csv using a representative WoAIS config
python tools/trace_generator.py

# Specify an output path
python tools/trace_generator.py path/to/output.csv
```

The demo config produces a 600-second trace with a 10:30:60 QUERY/INTERACTIVE/BATCH mix, varied DAG shapes, and three label columns: `op_type`, `file_size_mb`, and `noisy_cpu_seconds` (log-normal noise, ~20% std). It is a good starting point for general scheduler evaluation.

### Programmatic — import and configure

For anything beyond the demo, write a short Python script that imports the generator and builds a `TraceConfig`. Run it with `python my_script.py`. Examples are in the sections below.

---

## Core concepts

### SegmentProfile

Defines the resource requirements for one operator — the building block of every pipeline.

```python
SegmentProfile(
    name="my_profile",
    baseline_cpu_seconds=10,   # CPU time at 1 core
    cpu_scaling="linear3",     # how time shrinks with more cores
    storage_read_gb=30,        # GB read from storage (also sets peak memory if memory_gb is None)
    memory_gb=None,            # explicit peak memory override; None = same as storage_read_gb
)
```

Seven defaults are built in, mirroring Eudoxia's internal segment prototypes:

| Key | cpu_seconds | cpu_scaling | storage_read_gb | Character |
|---|---|---|---|---|
| `io_heavy` | 1 | const | 55 | Almost pure I/O |
| `io_med_a` | 2 | sqrt | 55 | I/O-bound |
| `io_med_b` | 5 | linear3 | 45 | Mostly I/O |
| `balanced` | 15 | linear3 | 37.5 | Balanced |
| `cpu_med` | 20 | linear7 | 30 | Mostly CPU |
| `cpu_heavy` | 40 | linear7 | 20 | CPU-bound |
| `cpu_intensive` | 80 | squared | 10 | Almost pure CPU |
| `query` | 15 | linear3 | 35 | Used by QUERY pipelines |

Valid `cpu_scaling` values: `const`, `log`, `sqrt`, `linear3`, `linear7`, `squared`, `exp`.

### PipelineSpec

Describes one class of pipelines — priority, weight in the arrival mix, operator count, DAG shape, and which resource profile each operator gets.

Three convenience constructors cover the common cases:

```python
PipelineSpec.query(weight=0.1)
# Always 1 operator, linear DAG, uses the 'query' profile.

PipelineSpec.interactive(
    num_ops_mean=5,      # average number of operators per pipeline
    num_ops_min=2,       # never fewer than this
    num_ops_max=8,       # never more than this (None = no cap)
    weight=0.3,
    cpu_io_ratio=0.5,    # 0.0 = IO-heavy operators, 1.0 = CPU-heavy operators
    dag_shapes=["linear", "branch_in", "branch_out"],
    dag_shape_weights=[0.7, 0.15, 0.15],
)

PipelineSpec.batch(
    num_ops_mean=8,
    num_ops_min=5,
    num_ops_max=None,    # no upper cap
    weight=0.6,
    cpu_io_ratio=0.3,    # slightly IO-heavy
)
```

### ArrivalPattern

Controls the regular cadence of pipeline arrivals — a batch of pipelines arrives every `waiting_seconds_mean` seconds on average.

```python
ArrivalPattern(
    waiting_seconds_mean=10.0,       # average gap between batches (seconds)
    waiting_seconds_stdev=2.5,       # jitter; defaults to mean/4 if omitted
    num_pipelines_per_batch=4,       # pipelines per arrival event
)
```

### BurstSpec

Injects a fixed number of pipelines at a specific simulation time, independent of the regular arrival cadence. Used to model sudden load spikes.

```python
BurstSpec(
    time_seconds=120.0,    # when the burst arrives
    num_pipelines=20,      # how many pipelines arrive at once
    spec_index=0,          # which PipelineSpec to use (None = sample proportionally)
)
```

### LabelSpec

Attaches extra metadata columns to each operator. Five built-in generators are provided:

```python
op_type_label()
# Emits 'read', 'write', or 'transform' based on DAG position.

file_size_mb_label(lo=50, hi=500)
# Random file size for read/write ops; empty for transforms.

fixed_label("experiment", "condition_A")
# Always the same value — useful for tagging experiment conditions.

choice_label("tier", ["hot", "warm", "cold"], weights=[0.2, 0.5, 0.3])
# Weighted random choice from a list.

noisy_label(
    "noisy_cpu_seconds",
    true_value_fn=lambda ctx, rng: ctx.profile.baseline_cpu_seconds,
    noise_fn=lambda v, rng: v * float(rng.lognormal(0, 0.2)),
)
# Ground-truth value with noise applied — the core primitive for noisy oracle experiments.
```

The `OperatorContext` passed to label generators exposes: `op_index`, `num_ops`, `dag_shape`, `is_root`, `is_leaf`, `priority`, `profile`, `pipeline_id`.

### TraceConfig

The top-level object that wires everything together.

```python
TraceConfig(
    duration_seconds=600,
    ticks_per_second=1000,      # must match Eudoxia simulation params
    pipeline_specs=[...],
    arrival_pattern=ArrivalPattern(...),
    random_seed=42,             # fully reproducible given the same seed
    bursts=[...],               # optional; empty by default
)
```

---

## Usage examples

### General scheduler evaluation trace

A realistic mixed workload close to the WoAIS canonical parameters:

```python
from tools.trace_generator import (
    TraceConfig, TraceGenerator, ArrivalPattern, PipelineSpec,
    op_type_label, file_size_mb_label,
)

config = TraceConfig(
    duration_seconds=600,
    ticks_per_second=1000,
    pipeline_specs=[
        PipelineSpec.query(weight=0.1),
        PipelineSpec.interactive(num_ops_mean=5, weight=0.3),
        PipelineSpec.batch(num_ops_mean=8, weight=0.6,
                           label_specs=[op_type_label(), file_size_mb_label()]),
    ],
    arrival_pattern=ArrivalPattern(waiting_seconds_mean=10.0, num_pipelines_per_batch=4),
    random_seed=42,
)
TraceGenerator(config).write_csv("traces/general.csv")
```

### Priority stress test with a QUERY burst

Tests whether the scheduler correctly handles a sudden spike of high-priority work arriving mid-simulation:

```python
from tools.trace_generator import (
    TraceConfig, TraceGenerator, ArrivalPattern, PipelineSpec, BurstSpec,
)

config = TraceConfig(
    duration_seconds=600,
    ticks_per_second=1000,
    pipeline_specs=[
        PipelineSpec.query(weight=0.1),           # spec_index=0
        PipelineSpec.batch(weight=0.9,
                           num_ops_mean=10,
                           num_ops_min=6),
    ],
    arrival_pattern=ArrivalPattern(waiting_seconds_mean=15.0, num_pipelines_per_batch=3),
    bursts=[
        BurstSpec(time_seconds=120, num_pipelines=20, spec_index=0),  # QUERY spike at t=120s
        BurstSpec(time_seconds=300, num_pipelines=20, spec_index=0),  # second spike at t=300s
    ],
    random_seed=42,
)
TraceGenerator(config).write_csv("traces/priority_burst.csv")
```

### Noisy oracle experiment

Adds a `noisy_cpu_seconds` label column with log-normal noise at a configurable noise level. Estimators trained on this column will see corrupted hints:

```python
from tools.trace_generator import (
    TraceConfig, TraceGenerator, ArrivalPattern, PipelineSpec, noisy_label,
)

def make_noisy_trace(noise_std: float, output: str):
    noisy_cpu = noisy_label(
        "noisy_cpu_seconds",
        true_value_fn=lambda ctx, rng: ctx.profile.baseline_cpu_seconds,
        noise_fn=lambda v, rng: round(v * float(rng.lognormal(0, noise_std)), 3),
    )
    config = TraceConfig(
        duration_seconds=600,
        ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.1, label_specs=[noisy_cpu]),
            PipelineSpec.interactive(weight=0.3, label_specs=[noisy_cpu]),
            PipelineSpec.batch(weight=0.6, label_specs=[noisy_cpu]),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=10.0),
        random_seed=42,
    )
    TraceGenerator(config).write_csv(output)

make_noisy_trace(noise_std=0.1, output="traces/noisy_10pct.csv")   # low noise
make_noisy_trace(noise_std=0.3, output="traces/noisy_30pct.csv")   # medium noise
make_noisy_trace(noise_std=0.6, output="traces/noisy_60pct.csv")   # high noise
```

### CPU-heavy workload (scheduler resource allocation stress)

Forces schedulers to manage CPU contention rather than memory contention:

```python
from tools.trace_generator import (
    TraceConfig, TraceGenerator, ArrivalPattern, PipelineSpec,
)

config = TraceConfig(
    duration_seconds=600,
    ticks_per_second=1000,
    pipeline_specs=[
        PipelineSpec.batch(
            weight=1.0,
            num_ops_mean=6,
            num_ops_min=4,
            num_ops_max=8,
            cpu_io_ratio=1.5,   # strongly CPU-heavy profiles
        ),
    ],
    arrival_pattern=ArrivalPattern(waiting_seconds_mean=8.0, num_pipelines_per_batch=5),
    random_seed=42,
)
TraceGenerator(config).write_csv("traces/cpu_heavy.csv")
```

### Fixed operator count (deterministic pipeline shape)

Setting `num_ops_min` and `num_ops_max` to the same value makes every pipeline exactly that length:

```python
PipelineSpec.batch(num_ops_mean=5, num_ops_min=5, num_ops_max=5)
```

---

## How it's implemented

The generator runs a tick loop from `0` to `duration × ticks_per_second`. On each tick it:

1. Checks whether any `BurstSpec` times have been reached and emits those pipelines immediately.
2. Checks whether the regular inter-arrival gap has elapsed and emits a normal batch if so.
3. For each pipeline, samples a `PipelineSpec` by weight, draws an operator count, picks a DAG shape, builds the parent map (`_build_parent_map`), selects a `SegmentProfile` per operator, calls each `LabelSpec` generator, and accumulates `TraceRow` objects.

`write_csv()` serializes the rows to CSV, auto-discovering all label column names and writing them in sorted order after the nine fixed columns.

All randomness flows through a single `numpy.random.Generator` seeded from `TraceConfig.random_seed`, so traces are fully reproducible.
