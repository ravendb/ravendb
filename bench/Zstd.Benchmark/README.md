# Zstd.Benchmark

Measures how RavenDB uses zstd, through the production code paths (`Sparrow.Utils.ZstdLib` and `Sparrow.Utils.ZstdStream`),
on realistic data. Use it before and after any change to the zstd binding, `ZstdStream`, or the native `libzstd` binaries.

## What is measured

| Benchmark | Production code it models | Parameters |
|---|---|---|
| `DocumentCompressionBenchmarks` | Voron document compression: `TableValueCompressor` / `Table` calling `ZstdLib.Compress` / `Decompress` per document on the thread-static context | dataset, with/without trained dictionary |
| `DictionaryTrainingBenchmarks` | `TableValueCompressor.MaybeTrainCompressionDictionary`: ZDICT training + CDict/DDict digestion | dataset |
| `LargeStreamBenchmarks` | Exports, logical and snapshot backups, restores, bulk insert: one long `ZstdStream`, 32KB async writes/reads | JSON vs blittable payload, Fastest/Optimal, in-memory vs syscall-per-call target |
| `SmallStreamBenchmarks` | HTTP bodies: a new `ZstdStream` per request/response at `Fastest` (server `ZstdCompressionProvider`, client `BlittableJsonContent`, `RequestExecutor`) | body size |
| `FlushingStreamBenchmarks` | `ReadWriteCompressedStream` on replication / subscription / cluster TCP: long-lived stream flushed after every batch | batch size, target |
| `ParallelCompressionBenchmarks` | `ZstdStream` with worker threads (`ZSTD_c_nbWorkers`), the candidate for backups and exports; needs a multi-threaded libzstd | payload, level, workers |
| `DocumentCompressionVariantsBenchmarks` | Raw libzstd ways of compressing with a dictionary without writing the dictionary id into every frame | dataset, variant |

Methods named `*_Baseline` run a frozen copy of the code as it was before the zstd optimizations (`Baseline/BaselineZstdStream.cs`,
and the old `ZstdLib.Compress` calls), next to the production code, in the same process. Changes to the code are therefore
measured within one run, and library versions are compared as jobs of one run - never as two separate runs on their own.

The `report` command adds what BenchmarkDotNet cannot show: compressed sizes and ratios, zstd frame header overhead per
document (including the dictionary ID bytes), how many calls `ZstdStream` makes into the stream below it, and a level sweep.

### Data

- **Documents**: Northwind collections from `src/Raven.Server/Web/Studio/EmbeddedData/Northwind.ravendbdump`
  (`Orders`, `Companies`), aggregate documents built from them (`CompanyWithOrders`: a company with its orders embedded),
  and `bench/Micro.Benchmark/Data/monsters.json` (`Monsters`). Documents are converted to blittable, the format Voron compresses.
  Like production, the dictionary is trained on up to 256 recent documents (max 32KB each, 1MB total, one Voron page)
  and then used on *other* documents: training and measured documents come from disjoint halves of the source data.
  Small collections are grown to 1024 measured documents with mutated variants (new ids, shifted dates, perturbed numbers,
  re-rolled digit runs; text without digits is kept, as real collections repeat their vocabulary).
- **Streams**: the real decompressed Northwind export (documents, revisions, binary attachments) followed by mutated copies
  of its documents (JSON payload), or the same documents as concatenated blittables (blittable payload).
  Generated deterministically and cached in `%TEMP%/ravendb-zstd-bench`.
- **Stream targets**: `Memory` measures compression plus call overhead; `Syscall` uses unbuffered OS handles (the null
  device for writes, a page-cached temp file for reads) so every inner `Read`/`Write` is a real system call, as with sockets and files.

Every benchmark verifies a round trip during setup, so a broken binary or binding fails instead of producing fast wrong numbers.

## Running

```bash
dotnet build bench/Zstd.Benchmark -c Release
bench/Zstd.Benchmark/bin/Release/net10.0/Zstd.Benchmark run --artifacts .agents/zstd-bench/<label>
```

Or use `run.ps1` (`-Label`, `-Filter`, `-Lib`, `-Quick`), which also writes the `report` output next to the results.

- `--filter *Document*` (or any other BenchmarkDotNet argument) narrows the run; default is everything (~20 minutes per library).
- `--lib <id>=<path>` (repeatable) runs every benchmark once per binary, each in its own process; the first is the baseline
  and BenchmarkDotNet adds Ratio columns. Each process verifies it loaded the expected version.
- `--quick` is for smoke testing only (1 process, 5 iterations).

### Getting stable numbers

- Plug the laptop in and use the best performance power mode; close other heavy applications, and don't build or run
  tests while benchmarks run (a later run rebuilds the code at its start, so don't edit sources between repeated runs either).
- On hybrid CPUs (P-cores + E-cores) the processes are pinned to performance cores by default (`--affinity pcores`).
- Defaults are 2 processes x 15 iterations per case.
- **Run everything twice** and only trust effects both runs agree on (`compare ... --confirm-with <second run>`).
  On the laptop this harness was built on, an A/A test (same code, two runs) flagged 18 of 46 cases as different by up to
  33% between runs, while in-run comparisons of identical code stayed within about +-5% (Flushing benchmarks: up to 34%).

## Comparing

```bash
# frozen pre-change code vs production code, same run, confirmed by a second run
Zstd.Benchmark compare run1 --in-run --confirm-with run2

# library A vs B (two jobs of the same run)
Zstd.Benchmark compare run1 --baseline-job shipped --candidate-job 1.5.7 --confirm-with run2

# everything at once: old code on the old library vs new code on the new library
Zstd.Benchmark compare run1 --in-run --baseline-job shipped --candidate-job 1.5.7 --confirm-with run2

# two separate runs (avoid - see above)
Zstd.Benchmark compare before after --confirm-with "before2;after2"
```

Per run, a result is reported as faster/slower only when the 99.9% confidence intervals do not overlap and the means differ
by more than `--threshold` (default 3%). With `--confirm-with`, an effect is confirmed only when both runs give the same
verdict; disagreements are reported as inconsistent.

## Building libzstd variants

`native-build/` builds libzstd with the production build functions (`src/Raven.Pal/build-libs/zstd.sh`) in the same
Ubuntu 18.04 toolchain as `zstd-build.Dockerfile` (minus the macOS stage, which needs the private SDK tarball).
Rebuilding v1.4.4 this way reproduces the shipped Linux/ARM binaries byte for byte, and the Windows ones up to the PE timestamp.

```bash
docker build -t ravendb-zstd-bench-build bench/Zstd.Benchmark/native-build
# /scripts: build scripts, /zstd-src: a zstd clone holding the refs, /out: output
MSYS_NO_PATHCONV=1 docker run --rm \
    -v "$PWD/src/Raven.Pal/build-libs:/scripts:ro" \
    -v "<facebook/zstd clone>:/zstd-src:ro" \
    -v "<output dir>:/out" \
    -v "$PWD/bench/Zstd.Benchmark/native-build:/bench:ro" \
    ravendb-zstd-bench-build bash /bench/build-variants.sh 1.5.7=v1.5.7 1.5.7-Os=v1.5.7:win64os
```

## Reports

```bash
Zstd.Benchmark report [--lib path/to/libzstd.win.x64.dll] [--out report.md] [--no-sweep]
Zstd.Benchmark libinfo --lib a=path1 --lib b=path2
```
