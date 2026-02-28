# bwm

`bwm` is a C++23 benchmark harness for evaluating bandwidth-sensitive data paths across disk and TCP loopback media, with raw and compressed flows (LZ4/Zstd), deterministic data generation, sink-side integrity tracking, and repeatable metric reporting.

## Project Goals

1. **Measure effective throughput correctly** for:
   - raw write
   - compressed write
   - raw read
   - decompressed read
2. **Preserve correctness under load** via per-chunk framing and checksum validation.
3. **Support disk and TCP loopback** execution paths with shared reporting semantics.
4. **Provide deterministic, repeatable runs** using seed-driven synthetic data generation.
5. **Produce both human and JSON summaries** suitable for automation and regression tracking.

## Functional Requirements

- C++23 build with Meson.
- Runtime codec support:
  - `none`
  - `lz4`
  - `zstd`
- Medium contracts and implementations:
  - `make_disk_medium(...)`
  - `make_tcp_sender_medium(...)`
  - `make_tcp_receiver_medium(...)`
- Scheduler contract and implementation:
  - `make_scheduler(...)`
- Protocol runner wiring:
  - `run_timed_phase(...)` is fully implemented and delegates to runtime execution.
- Two-process TCP loopback operation for benchmark orchestration (`sender` + `receiver`).
- Validation-friendly tests (unit + integration), without smoke-only tests.

## Non-Functional Requirements

- Deterministic generation for the same seed + sequence.
- Clear error propagation using `bwm::Error` with explicit error metadata (`code`, `message`).
- No placeholder/stub protocol path in production benchmark flow.
- Bounded queue behavior for scheduler implementation.
- Direct-I/O checks and platform-specific sync behavior in disk benchmark flow.

## Build and Test

```bash
meson setup build --buildtype=release -Dcpp_std=c++23
meson compile -C build
meson test -C build --print-errorlogs
```

If the build directory already exists, reconfigure after Meson file updates:

```bash
meson setup build --reconfigure
```

## CLI Usage

### Common

```bash
./build/bwm \
  --medium disk|tcp \
  --mode raw_write|comp_write|raw_read|decomp_read|all \
  --algo none|lz4|zstd \
  --duration-sec 10 \
  --repeats 5 \
  --threads 1 \
  --chunk-size 1048576 \
  --queue-depth 1024 \
  --seed 1
```

### Disk suite example

```bash
./build/bwm --medium disk --mode all --algo lz4 --duration-sec 1 --repeats 1 --output-dir ./bwm_out --json ./out.json
```

### TCP suite example

```bash
./build/bwm --medium tcp --mode all --algo lz4 --duration-sec 1 --repeats 1 --port 9321 --json ./out.json
```

## Output Metrics

Human and JSON output include:

- `W_eff_raw_GBps`
- `W_eff_cmp_GBps`
- `R_eff_raw_GBps`
- `R_eff_dec_GBps`
- `M_write`
- `M_read`
- `CR_comp_write`
- `CR_decomp_read`

Each metric summary reports distribution statistics (`mean`, `median`, `p95`, `min`, `max`).

## Architecture Overview

- `src/main.cpp` + `src/app/engine.cpp`: thin entry path.
- `src/app/runtime_runner.cpp`: benchmark orchestration and CLI dispatch.
- `src/codec/*`: runtime codec implementation and interface adapter.
- `src/generator/*`: deterministic low-entropy generator.
- `src/sink/*`: xxHash sink for integrity tracking.
- `src/medium/medium.cpp`: concrete disk and TCP medium factories/implementations.
- `src/scheduler/scheduler.cpp`: bounded worker scheduler implementation.
- `src/bench/protocol.cpp`: protocol-level timed phase execution and aggregate stats.

## Protocol and Framing Notes

- Compressed flows use chunk frame metadata (`raw_size`, `comp_size`, checksum, algo id).
- Read paths verify checksum correctness before sink aggregation.
- TCP sender emits an explicit terminal header marker (`raw_size==0 && comp_size==0`).

## Implementation Directives

1. **Do not reintroduce smoke-only tests.** New tests must validate behavior/metrics/contracts.
2. **Keep `engine.cpp` thin.** Orchestration belongs in dedicated modules.
3. **Use explicit `bwm::Error` values for operational failures**; avoid hidden exception-only control paths.
4. **Preserve deterministic behavior** for benchmark comparability.
5. **Maintain protocol metrics compatibility** with JSON consumers.

## Test Suite Inventory

Unit tests:

- `codec_roundtrip_tests`
- `generator_determinism_tests`
- `protocol_metrics_tests`
- `medium_factory_tests`
- `scheduler_factory_tests`
- `protocol_runner_tests`

Integration tests:

- `disk_e2e_validation`
- `tcp_loopback_e2e_validation`
