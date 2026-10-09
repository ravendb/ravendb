using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Sparrow.Utils;
using Zstd.Benchmark.Baseline;
using Zstd.Benchmark.Infrastructure;

namespace Zstd.Benchmark.Benchmarks
{
    /// <summary>
    /// One short-lived <see cref="ZstdStream"/> per payload: HTTP request and response bodies (server ZstdCompressionProvider,
    /// client BlittableJsonContent / RequestExecutor). Context creation and teardown are part of every operation, as in production.
    /// Every operation is measured for the production stream and for the frozen pre-change copy (<c>*_Baseline</c>) in the same run.
    /// </summary>
    [BenchmarkCategory("SmallStream")]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    [CategoriesColumn]
    public class SmallStreamBenchmarks : IThroughputSource
    {
        // Both the server response default (Http.ZstdResponseCompressionLevel) and client request compression use Fastest
        private const CompressionLevel Level = CompressionLevel.Fastest;
        private const int ReadChunkSize = 16 * 1024;

        private byte[] _payload;
        private byte[] _compressed;
        private byte[] _readBuffer;
        private MemoryStream _sink;

        [Params(1024, 16 * 1024, 256 * 1024)]
        public int PayloadSize { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            NativeLibrarySelector.EnsureExpectedLibrary();

            _payload = Datasets.GetPayload(PayloadKind.Json, LargeStreamBenchmarks.PayloadSize).AsSpan(0, PayloadSize).ToArray();
            _compressed = StreamCodec.Compress(_payload, Level, PayloadSize);
            StreamCodec.VerifyRoundTrip(_compressed, _payload);
            _readBuffer = new byte[ReadChunkSize];
            _sink = new MemoryStream(_compressed.Length * 2 + 1024);
        }

        [Benchmark(Baseline = true), BenchmarkCategory("Compress")]
        public Task<long> CompressAsync_Baseline() => CompressCore(BaselineZstdStream.Compress(ResetSink(), Level, leaveOpen: true));

        [Benchmark, BenchmarkCategory("Compress")]
        public Task<long> CompressAsync() => CompressCore(ZstdStream.Compress(ResetSink(), Level, leaveOpen: true));

        [Benchmark(Baseline = true), BenchmarkCategory("Decompress")]
        public Task<long> DecompressAsync_Baseline() => DecompressCore(BaselineZstdStream.Decompress(new MemoryStream(_compressed, writable: false)));

        [Benchmark, BenchmarkCategory("Decompress")]
        public Task<long> DecompressAsync() => DecompressCore(ZstdStream.Decompress(new MemoryStream(_compressed, writable: false)));

        private MemoryStream ResetSink()
        {
            _sink.Position = 0;
            _sink.SetLength(0);
            return _sink;
        }

        private async Task<long> CompressCore(Stream zstd)
        {
            await using (zstd)
                await zstd.WriteAsync(_payload);
            return _sink.Length;
        }

        private async Task<long> DecompressCore(Stream zstd)
        {
            long total = 0;
            await using (zstd)
            {
                int read;
                while ((read = await zstd.ReadAsync(_readBuffer)) != 0)
                    total += read;
            }

            if (total != _payload.Length)
                throw new InvalidOperationException($"Decompressed {total} bytes, expected {_payload.Length}");

            return total;
        }

        public double GetUncompressedBytesPerOperation(string method) => PayloadSize;
    }
}
