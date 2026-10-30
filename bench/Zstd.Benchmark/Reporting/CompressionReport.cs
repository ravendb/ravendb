using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Text;
using Sparrow.Utils;
using Zstd.Benchmark.Infrastructure;

namespace Zstd.Benchmark.Reporting
{
    /// <summary>
    /// Deterministic measurements BenchmarkDotNet does not capture: compressed sizes, zstd frame overhead and how many calls
    /// <see cref="ZstdStream"/> makes into the stream below it. Uses the production code paths and the selected libzstd binary.
    /// </summary>
    internal static unsafe class CompressionReport
    {
        private const int ChunkSize = 32 * 1024;

        public static string Create(bool includeLevelSweep)
        {
            StringBuilder sb = new();
            ZstdNativeLibrary library = NativeLibrarySelector.Current;
            sb.AppendLine($"# zstd compression report").AppendLine();
            sb.AppendLine($"- {library.Describe()}");
            sb.AppendLine($"- {DateTime.UtcNow:u}, {Environment.MachineName}, {Environment.ProcessorCount} logical processors").AppendLine();

            AppendDocuments(sb);
            AppendStreams(sb);
            AppendSmallStreams(sb);
            AppendFlushing(sb);
            AppendContextSizes(sb, library);
            AppendSmallestSizeCandidates(sb, library);
            if (includeLevelSweep)
                AppendLevelSweep(sb, library);

            return sb.ToString();
        }

        private static void AppendDocuments(StringBuilder sb)
        {
            sb.AppendLine("## Documents (ZstdLib.Compress per document, as Voron tables)").AppendLine();
            sb.AppendLine("| Dataset | Docs | Avg raw B | Dictionary | Dict size B | Avg compressed B | Ratio | Avg frame header B | Dict-ID B/doc | Content-size B/doc |");
            sb.AppendLine("|---|---:|---:|---|---:|---:|---:|---:|---:|---:|");

            foreach (string dataset in Datasets.DocumentDatasets)
            {
                DocumentCorpus corpus = Datasets.GetCorpus(dataset);
                byte[] trained = DictionaryTrainer.Train(corpus);
                foreach (bool useDictionary in new[] { false, true })
                {
                    using ZstdLib.CompressionDictionary dictionary = useDictionary
                        ? DictionaryTrainer.CreateDictionary(trained, id: 1)
                        : DictionaryTrainer.CreateEmptyDictionary();

                    long compressedTotal = 0, headerTotal = 0, dictIdTotal = 0, contentSizeTotal = 0;
                    foreach (byte[] document in corpus.Documents)
                    {
                        byte[] output = new byte[ZstdLib.GetMaxCompression(document.Length) + 32];
                        int size;
                        fixed (byte* src = document)
                        fixed (byte* dst = output)
                            size = ZstdLib.Compress(src, document.Length, dst, output.Length, dictionary);

                        FrameHeader header = FrameHeader.Parse(output.AsSpan(0, size));
                        compressedTotal += size;
                        headerTotal += header.Size;
                        dictIdTotal += header.DictionaryIdSize;
                        contentSizeTotal += header.ContentSizeFieldSize;
                    }

                    int count = corpus.Documents.Length;
                    sb.AppendLine(string.Create(CultureInfo.InvariantCulture,
                        $"| {dataset} | {count} | {corpus.AverageDocumentSize:N0} | {(useDictionary ? "trained" : "none")} | {(useDictionary ? trained.Length : 0):N0} | " +
                        $"{(double)compressedTotal / count:N1} | {(double)corpus.DocumentsBytes / compressedTotal:N3} | {(double)headerTotal / count:N2} | " +
                        $"{(double)dictIdTotal / count:N2} | {(double)contentSizeTotal / count:N2} |"));
                }
            }

            sb.AppendLine();
        }

        private static void AppendStreams(StringBuilder sb)
        {
            sb.AppendLine($"## Large streams (ZstdStream, {LargeStreamPayloadMb} MiB, {ChunkSize / 1024}KB writes / reads)").AppendLine();
            sb.AppendLine("| Payload | Level | Compressed B | Ratio | Compress: inner writes | Avg write B | Decompress: inner reads | Avg read B |");
            sb.AppendLine("|---|---|---:|---:|---:|---:|---:|---:|");

            foreach (PayloadKind kind in new[] { PayloadKind.Json, PayloadKind.Blittable })
            {
                byte[] payload = Datasets.GetPayload(kind, Benchmarks.LargeStreamBenchmarks.PayloadSize);
                foreach (CompressionLevel level in new[] { CompressionLevel.Fastest, CompressionLevel.Optimal, CompressionLevel.SmallestSize })
                {
                    // level 22 is very slow, use a slice so the report stays quick; ratios are still representative
                    ReadOnlySpan<byte> input = level == CompressionLevel.SmallestSize ? payload.AsSpan(0, 4 * 1024 * 1024) : payload;

                    CountingStream writeCounter = new(new MemoryStream());
                    StreamCodec.Compress(input, level, ChunkSize, writeCounter);

                    byte[] compressed = StreamCodec.Compress(input, level, ChunkSize);
                    CountingStream readCounter = new(new MemoryStream(compressed, writable: false));
                    StreamCodec.Decompress(readCounter, ChunkSize, input.Length);

                    string label = level == CompressionLevel.SmallestSize ? $"{level} (4 MiB slice)" : level.ToString();
                    sb.AppendLine(string.Create(CultureInfo.InvariantCulture,
                        $"| {kind} | {label} | {compressed.Length:N0} | {(double)input.Length / compressed.Length:N3} | {writeCounter.Writes:N0} | " +
                        $"{(double)writeCounter.BytesWritten / Math.Max(1, writeCounter.Writes):N0} | {readCounter.Reads:N0} | {(double)readCounter.BytesRead / Math.Max(1, readCounter.Reads):N0} |"));
                }
            }

            sb.AppendLine();
        }

        private static int LargeStreamPayloadMb => Benchmarks.LargeStreamBenchmarks.PayloadSize / (1024 * 1024);

        private static void AppendSmallStreams(StringBuilder sb)
        {
            sb.AppendLine("## HTTP-sized bodies (one ZstdStream per body, Fastest)").AppendLine();
            sb.AppendLine("| Payload B | Compressed B | Ratio | Inner writes | Inner reads (decompress, 16KB reader) |");
            sb.AppendLine("|---:|---:|---:|---:|---:|");

            byte[] payload = Datasets.GetPayload(PayloadKind.Json, Benchmarks.LargeStreamBenchmarks.PayloadSize);
            foreach (int size in new[] { 1024, 16 * 1024, 256 * 1024 })
            {
                ReadOnlySpan<byte> input = payload.AsSpan(0, size);
                CountingStream writeCounter = new(new MemoryStream());
                StreamCodec.Compress(input, CompressionLevel.Fastest, size, writeCounter);
                byte[] compressed = StreamCodec.Compress(input, CompressionLevel.Fastest, size);
                CountingStream readCounter = new(new MemoryStream(compressed, writable: false));
                StreamCodec.Decompress(readCounter, 16 * 1024, size);

                sb.AppendLine(string.Create(CultureInfo.InvariantCulture,
                    $"| {size:N0} | {compressed.Length:N0} | {(double)size / compressed.Length:N3} | {writeCounter.Writes:N0} | {readCounter.Reads:N0} |"));
            }

            sb.AppendLine();
        }

        private static void AppendFlushing(StringBuilder sb)
        {
            sb.AppendLine("## Flushed connection streams (ZstdStream, Optimal, flush after every batch, 8 MiB blittable)").AppendLine();
            sb.AppendLine("| Batch B | Compressed B | Ratio | Inner writes | Avg write B |");
            sb.AppendLine("|---:|---:|---:|---:|---:|");

            byte[] payload = Datasets.GetPayload(PayloadKind.Blittable, Benchmarks.LargeStreamBenchmarks.PayloadSize).AsSpan(0, 8 * 1024 * 1024).ToArray();
            foreach (int batch in new[] { 4 * 1024, 64 * 1024 })
            {
                MemoryStream output = new();
                CountingStream counter = new(output);
                using (ZstdStream zstd = ZstdStream.Compress(counter, CompressionLevel.Optimal, leaveOpen: true))
                {
                    for (int offset = 0; offset < payload.Length; offset += batch)
                    {
                        zstd.Write(payload.AsSpan(offset, Math.Min(batch, payload.Length - offset)));
                        zstd.Flush();
                    }
                }

                sb.AppendLine(string.Create(CultureInfo.InvariantCulture,
                    $"| {batch:N0} | {output.Length:N0} | {(double)payload.Length / output.Length:N3} | {counter.Writes:N0} | {(double)counter.BytesWritten / Math.Max(1, counter.Writes):N0} |"));
            }

            sb.AppendLine();
        }

        private static void AppendContextSizes(StringBuilder sb, ZstdNativeLibrary library)
        {
            const int inputSize = 4 * 1024 * 1024;
            sb.AppendLine($"## Native context sizes after streaming {inputSize / (1024 * 1024)} MiB of unknown length (what a pooled context retains)").AppendLine();
            sb.AppendLine("| Level | Workers | CCtx B | DCtx B (decompressing that frame) |");
            sb.AppendLine("|---:|---:|---:|---:|");

            byte[] input = Datasets.GetPayload(PayloadKind.Json, Benchmarks.LargeStreamBenchmarks.PayloadSize).AsSpan(0, inputSize).ToArray();
            foreach ((int level, int workers) in new[] { (1, 0), (3, 0), (9, 0), (19, 0), (22, 0), (3, 4) })
            {
                if (workers > 0 && library.SupportsMultithreading == false)
                    continue;

                byte[] output = new byte[library.CompressBound(input.Length)];
                void* cctx = library.CreateCCtx();
                long cctxSize;
                int compressedSize;
                try
                {
                    library.Check(library.CCtxSetParameter(cctx, 100, level));
                    if (workers > 0)
                        library.Check(library.CCtxSetParameter(cctx, 400, workers));

                    fixed (byte* src = input)
                    fixed (byte* dst = output)
                    {
                        ZstdNativeLibrary.StreamBuffer inBuffer = new() { Data = src, Size = (UIntPtr)input.Length };
                        ZstdNativeLibrary.StreamBuffer outBuffer = new() { Data = dst, Size = (UIntPtr)output.Length };
                        library.Check(library.CompressStream2(cctx, &outBuffer, &inBuffer, 0));
                        while ((ulong)library.CompressStream2(cctx, &outBuffer, &inBuffer, 2) != 0)
                        {
                        }
                        compressedSize = (int)outBuffer.Position;
                    }

                    cctxSize = (long)library.SizeOfCCtx(cctx);
                }
                finally
                {
                    library.FreeCCtx(cctx);
                }

                void* dctx = library.CreateDCtx();
                long dctxSize;
                try
                {
                    byte[] decompressed = new byte[input.Length];
                    fixed (byte* src = output)
                    fixed (byte* dst = decompressed)
                    {
                        ZstdNativeLibrary.StreamBuffer inBuffer = new() { Data = src, Size = (UIntPtr)compressedSize };
                        ZstdNativeLibrary.StreamBuffer outBuffer = new() { Data = dst, Size = (UIntPtr)decompressed.Length };
                        library.Check(library.DecompressStream(dctx, &outBuffer, &inBuffer));
                    }

                    dctxSize = (long)library.SizeOfDCtx(dctx);
                }
                finally
                {
                    library.FreeDCtx(dctx);
                }

                sb.AppendLine(string.Create(CultureInfo.InvariantCulture, $"| {level} | {workers} | {cctxSize:N0} | {dctxSize:N0} |"));
            }

            sb.AppendLine();
        }

        private static void AppendSmallestSizeCandidates(StringBuilder sb, ZstdNativeLibrary library)
        {
            int size = Benchmarks.SmallestSizeCandidatesBenchmarks.PayloadSize;
            sb.AppendLine($"## CompressionLevel.SmallestSize candidates (streaming, {size / (1024 * 1024)} MiB)").AppendLine();
            sb.AppendLine("| Payload | Candidate | Compressed B | Ratio | vs level 22 | CCtx B | CCtx B, 2 workers | CCtx B, 4 workers | DCtx B (restore) |");
            sb.AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|");

            foreach (PayloadKind kind in new[] { PayloadKind.Json, PayloadKind.Blittable })
            {
                byte[] payload = Datasets.GetPayload(kind, Benchmarks.LargeStreamBenchmarks.PayloadSize).AsSpan(0, size).ToArray();
                byte[] output = new byte[library.CompressBound(size)];
                int? level22Size = null;
                foreach (Benchmarks.SmallestSizeCandidate candidate in Enum.GetValues<Benchmarks.SmallestSizeCandidate>())
                {
                    int compressed = Benchmarks.SmallestSizeCandidatesBenchmarks.Compress(library, payload, output, candidate, workers: 0, out long cctxSize);
                    level22Size ??= compressed;

                    string workers2 = "-", workers4 = "-";
                    if (library.SupportsMultithreading)
                    {
                        byte[] scratch = new byte[output.Length];
                        Benchmarks.SmallestSizeCandidatesBenchmarks.Compress(library, payload, scratch, candidate, workers: 2, out long size2);
                        Benchmarks.SmallestSizeCandidatesBenchmarks.Compress(library, payload, scratch, candidate, workers: 4, out long size4);
                        workers2 = size2.ToString("N0", CultureInfo.InvariantCulture);
                        workers4 = size4.ToString("N0", CultureInfo.InvariantCulture);
                    }

                    long dctxSize = DecompressionContextSize(library, output, compressed, size);
                    sb.AppendLine(string.Create(CultureInfo.InvariantCulture,
                        $"| {kind} | {candidate} | {compressed:N0} | {(double)size / compressed:N3} | {(double)compressed / level22Size.Value - 1:+0.0%;-0.0%;0.0%} | {cctxSize:N0} | {workers2} | {workers4} | {dctxSize:N0} |"));
                }
            }

            sb.AppendLine();
        }

        private static long DecompressionContextSize(ZstdNativeLibrary library, byte[] compressed, int compressedSize, int decompressedSize)
        {
            void* dctx = library.CreateDCtx();
            try
            {
                byte[] decompressed = new byte[decompressedSize];
                fixed (byte* src = compressed)
                fixed (byte* dst = decompressed)
                {
                    ZstdNativeLibrary.StreamBuffer inBuffer = new() { Data = src, Size = (UIntPtr)compressedSize };
                    ZstdNativeLibrary.StreamBuffer outBuffer = new() { Data = dst, Size = (UIntPtr)decompressed.Length };
                    while ((ulong)inBuffer.Position < (ulong)inBuffer.Size)
                        library.Check(library.DecompressStream(dctx, &outBuffer, &inBuffer));
                }

                return (long)library.SizeOfDCtx(dctx);
            }
            finally
            {
                library.FreeDCtx(dctx);
            }
        }

        private static void AppendLevelSweep(StringBuilder sb, ZstdNativeLibrary library)
        {
            const int sliceSize = 8 * 1024 * 1024;
            sb.AppendLine($"## Level sweep (raw libzstd one-shot ZSTD_compress2, {sliceSize / (1024 * 1024)} MiB slice; speed is a rough best-of-3, use BenchmarkDotNet for real numbers)").AppendLine();
            sb.AppendLine("| Payload | Level | Workers | Ratio | MB/s (rough) |");
            sb.AppendLine("|---|---:|---:|---:|---:|");

            int[] levels = { -7, -5, -3, -1, 1, 2, 3, 4, 6, 9, 12, 19 };
            foreach (PayloadKind kind in new[] { PayloadKind.Json, PayloadKind.Blittable })
            {
                byte[] slice = Datasets.GetPayload(kind, Benchmarks.LargeStreamBenchmarks.PayloadSize).AsSpan(0, sliceSize).ToArray();
                byte[] output = new byte[library.CompressBound(slice.Length)];
                foreach (int level in levels)
                {
                    if (level < library.MinLevel)
                        continue;
                    AppendSweepRow(sb, library, kind, slice, output, level, workers: 0);
                }

                if (library.SupportsMultithreading)
                {
                    foreach (int workers in new[] { 1, 2, 4, 8 })
                        AppendSweepRow(sb, library, kind, slice, output, level: 3, workers);
                }
            }

            sb.AppendLine();
        }

        private static void AppendSweepRow(StringBuilder sb, ZstdNativeLibrary library, PayloadKind kind, byte[] slice, byte[] output, int level, int workers)
        {
            int size = 0;
            double best = double.MaxValue;
            int runs = level >= 12 ? 1 : 3;
            for (int run = 0; run < runs; run++)
            {
                Stopwatch sw = Stopwatch.StartNew();
                size = library.Compress(slice, output, level, workers);
                best = Math.Min(best, sw.Elapsed.TotalSeconds);
            }

            sb.AppendLine(string.Create(CultureInfo.InvariantCulture,
                $"| {kind} | {level} | {workers} | {(double)slice.Length / size:N3} | {slice.Length / best / (1024 * 1024):N0} |"));
        }

        /// <summary>
        /// zstd frame header (RFC 8878, section 3.1.1.1).
        /// </summary>
        private readonly struct FrameHeader
        {
            public readonly int Size;
            public readonly int DictionaryIdSize;
            public readonly int ContentSizeFieldSize;

            private FrameHeader(int size, int dictionaryIdSize, int contentSizeFieldSize)
            {
                Size = size;
                DictionaryIdSize = dictionaryIdSize;
                ContentSizeFieldSize = contentSizeFieldSize;
            }

            public static FrameHeader Parse(ReadOnlySpan<byte> frame)
            {
                if (frame.Length < 6 || BitConverter.ToUInt32(frame) != 0xFD2FB528)
                    throw new InvalidDataException("Not a zstd frame");

                byte descriptor = frame[4];
                int contentSizeFlag = descriptor >> 6;
                bool singleSegment = ((descriptor >> 5) & 1) == 1;
                int dictionaryIdFlag = descriptor & 3;

                int dictionaryIdSize = dictionaryIdFlag switch { 0 => 0, 1 => 1, 2 => 2, _ => 4 };
                int contentSizeFieldSize = contentSizeFlag switch { 0 => singleSegment ? 1 : 0, 1 => 2, 2 => 4, _ => 8 };
                int windowDescriptorSize = singleSegment ? 0 : 1;

                return new FrameHeader(4 + 1 + windowDescriptorSize + dictionaryIdSize + contentSizeFieldSize, dictionaryIdSize, contentSizeFieldSize);
            }
        }
    }
}
