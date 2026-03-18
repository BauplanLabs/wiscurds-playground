=========================================================================
                          trace_generator.py
=========================================================================

What it is

  A standalone Python program that generates CSV workload trace files compatible with Eudoxia's replay format (eudoxia run params.toml -w trace.csv). The purpose
  is to create controlled, reproducible traces for scheduler experiments — where you can precisely dictate the workload mix, operator resource profiles, DAG
  structure, arrival timing, and most importantly, the label columns that carry extra per-operator metadata for schedulers and estimators to use.

  The standard Eudoxia trace has nine fixed columns. This generator writes those same nine columns, then appends any number of additional label columns after them.
   Eudoxia's reader treats those extra columns as the labels dict on each operator, so the traces are natively compatible with no changes to the simulator.

  ---
  How it's implemented

  The design is built around small, composable config objects that each control one axis of the trace independently.

  SegmentProfile defines the resource values for one operator — baseline_cpu_seconds, cpu_scaling, storage_read_gb, and optionally memory_gb. Seven default
  profiles are provided that mirror Eudoxia's internal segment prototypes, ranging from io_heavy (1 cpu-sec, 55GB scan) to cpu_intensive (80 cpu-sec, 10GB scan).

  OperatorContext is a read-only snapshot of an operator's situation — its index in the pipeline, total pipeline length, DAG shape, whether it's a root or leaf,
  priority, profile, and pipeline ID. It's passed to every label generator so labels can be context-aware.

  LabelSpec is the extensibility point for labels — just a column name and a Callable[[OperatorContext, rng], value]. Five built-in generators are provided:
  - op_type_label() — infers read/write/transform from DAG position and shape
  - file_size_mb_label(lo, hi) — random int for read/write ops, empty for transforms
  - fixed_label(col, val) — constant value, useful for tagging experiment conditions
  - choice_label(col, options, weights) — weighted random choice from a list
  - noisy_label(col, true_fn, noise_fn) — generates a ground-truth value then applies a noise function, which is the core primitive for noisy oracle experiments

  ArrivalPattern controls when batches of pipelines arrive — a normal distribution around waiting_seconds_mean with configurable standard deviation and batch size.

  PipelineSpec describes one class of pipelines — its priority, arrival weight, operator count distribution, DAG shape distribution, how to pick a resource profile
   per operator, and which label specs to apply. Convenience constructors PipelineSpec.query(), .interactive(), and .batch() cover the common cases with sensible
  defaults.

  TraceConfig is the single top-level object that wires everything together — duration, tick resolution, a list of PipelineSpecs, an ArrivalPattern, and a random
  seed.

  TraceGenerator does the actual work. generate() runs a tick loop from 0 to duration × ticks_per_second, fires arrival batches according to the pattern, samples a
   PipelineSpec by weight for each pipeline, builds the operator DAG using _build_parent_map() (which handles linear, branch_in, and branch_out shapes), calls the
  profile selector and each label generator per operator, and returns a flat list of TraceRow objects. write_csv() then serializes those to CSV — auto-discovering
  all label column names from the rows and writing them in sorted order after the nine fixed columns.

  All randomness flows through a single numpy.random.Generator seeded from TraceConfig.random_seed, so traces are fully reproducible.
