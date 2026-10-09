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
    /// Long streams through <see cref="ZstdStream"/>: exports, logical and snapshot backups, restores, bulk insert.
    /// The writer pushes 32KB chunks asynchronously, the reader pulls 32KB chunks - what the smuggler and backup code do.
    /// Every operation is measured for the production stream and for the frozen pre-change copy (<c>*_Baseline</c>) in the same run.
    /// </summary>
    [BenchmarkCategory("LargeStream")]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    [CategoriesColumn]
    public class LargeStreamBenchmarks : IThroughputSource
    {
        public const int PayloadSize = 32 * 1024 * 1024;
        private const int ChunkSize = 32 * 1024;

        private byte[] _payload;
        private byte[] _compressed;
        private byte[] _readBuffer;
        private MemoryStream _memorySink;
        private FileStream _nullSink;
        private FileStream _fileSource;

        [Params(PayloadKind.Json, PayloadKind.Blittable)]
        public PayloadKind Payload { get; set; }

        [Params(CompressionLevel.Fastest, CompressionLevel.Optimal)]
        public CompressionLevel Level { get; set; }

        [Params(StreamTarget.Memory, StreamTarget.Syscall)]
        public StreamTarget Target { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            NativeLibrarySelector.EnsureExpectedLibrary();

            _payload = Datasets.GetPayload(Payload, PayloadSize);
            _compressed = StreamCodec.Compress(_payload, Level, ChunkSize);
            StreamCodec.VerifyRoundTrip(_compressed, _payload);

            _readBuffer = new byte[ChunkSize];
            _memorySink = new MemoryStream(_compressed.Length * 2);
            _nullSink = TestStreams.OpenNullSink();
            _fileSource = TestStreams.OpenUnbufferedSource(_compressed);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _nullSink.Dispose();
            _fileSource.Dispose();
        }

        [Benchmark(Baseline = true), BenchmarkCategory("Compress")]
        public Task<long> Compress_Baseline() => CompressCore(sink => BaselineZstdStream.Compress(sink, Level, leaveOpen: true));

        [Benchmark, BenchmarkCategory("Compress")]
        public Task<long> Compress() => CompressCore(sink => ZstdStream.Compress(sink, Level, leaveOpen: true));

        [Benchmark(Baseline = true), BenchmarkCategory("Decompress")]
        public Task<long> Decompress_Baseline() => DecompressCore(source => BaselineZstdStream.Decompress(source, leaveOpen: true));

        [Benchmark, BenchmarkCategory("Decompress")]
        public Task<long> Decompress() => DecompressCore(source => ZstdStream.Decompress(source, leaveOpen: true));

        private async Task<long> CompressCore(Func<Stream, Stream> createCompressor)
        {
            Stream sink;
            if (Target == StreamTarget.Memory)
            {
                _memorySink.Position = 0;
                _memorySink.SetLength(0);
                sink = _memorySink;
            }
            else
            {
                sink = _nullSink;
            }

            await using (Stream zstd = createCompressor(sink))
            {
                for (int offset = 0; offset < _payload.Length; offset += ChunkSize)
                    await zstd.WriteAsync(_payload.AsMemory(offset, Math.Min(ChunkSize, _payload.Length - offset)));
            }

            return _payload.Length;
        }

        private async Task<long> DecompressCore(Func<Stream, Stream> createDecompressor)
        {
            Stream source;
            if (Target == StreamTarget.Memory)
            {
                source = new MemoryStream(_compressed, writable: false);
            }
            else
            {
                _fileSource.Position = 0;
                source = _fileSource;
            }

            long total = 0;
            await using (Stream zstd = createDecompressor(source))
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
