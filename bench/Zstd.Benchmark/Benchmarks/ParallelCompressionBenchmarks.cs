using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Sparrow.Utils;
using Zstd.Benchmark.Infrastructure;

namespace Zstd.Benchmark.Benchmarks
{
    /// <summary>
    /// <see cref="ZstdStream"/> with worker threads (ZSTD_c_nbWorkers), the candidate for backups and exports.
    /// Needs a libzstd built with ZSTD_MULTITHREAD; on a single-threaded binary the stream compresses on the calling thread.
    /// zstd splits the input into jobs of several window sizes (8MB at level 3), so parallelism only shows on long streams.
    /// </summary>
    [BenchmarkCategory("Parallel")]
    public class ParallelCompressionBenchmarks : IThroughputSource
    {
        private const int PayloadSize = 64 * 1024 * 1024;
        private const int ChunkSize = 32 * 1024;

        private byte[] _payload;
        private MemoryStream _sink;

        [Params(PayloadKind.Json, PayloadKind.Blittable)]
        public PayloadKind Payload { get; set; }

        [Params(CompressionLevel.Fastest, CompressionLevel.Optimal)]
        public CompressionLevel Level { get; set; }

        [Params(0, 1, 2, 4, 8)]
        public int Workers { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            NativeLibrarySelector.EnsureExpectedLibrary();

            _payload = Datasets.GetPayload(Payload, PayloadSize);
            _sink = new MemoryStream(PayloadSize / 2);

            using (ZstdStream probe = ZstdStream.Compress(Stream.Null, Level, leaveOpen: true, Workers))
            {
                probe.Write(_payload.AsSpan(0, 1024));
                Console.WriteLine($"// [zstd-bench] requested {Workers} workers, using {probe.Workers}");
            }

            _sink.Position = 0;
            _sink.SetLength(0);
            using (ZstdStream zstd = ZstdStream.Compress(_sink, Level, leaveOpen: true, Workers))
                zstd.Write(_payload);
            StreamCodec.VerifyRoundTrip(_sink.ToArray(), _payload);
        }

        [Benchmark]
        public async Task<long> Compress()
        {
            _sink.Position = 0;
            _sink.SetLength(0);

            await using (ZstdStream zstd = ZstdStream.Compress(_sink, Level, leaveOpen: true, Workers))
            {
                for (int offset = 0; offset < _payload.Length; offset += ChunkSize)
                    await zstd.WriteAsync(_payload.AsMemory(offset, Math.Min(ChunkSize, _payload.Length - offset)));
            }

            return _sink.Length;
        }

        public double GetUncompressedBytesPerOperation(string method) => PayloadSize;
    }
}
