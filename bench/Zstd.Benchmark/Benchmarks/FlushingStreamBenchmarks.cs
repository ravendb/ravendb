using System;
using System.IO;
using System.IO.Compression;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Sparrow.Utils;
using Zstd.Benchmark.Baseline;
using Zstd.Benchmark.Infrastructure;

namespace Zstd.Benchmark.Benchmarks
{
    /// <summary>
    /// Long-lived <see cref="ZstdStream"/> flushed after every batch: ReadWriteCompressedStream on replication, subscription and
    /// cluster TCP connections (default level, one context per connection, a flush per batch sent).
    /// Measured for the production stream and for the frozen pre-change copy (<c>*_Baseline</c>) in the same run.
    /// </summary>
    [BenchmarkCategory("Flushing")]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    public class FlushingStreamBenchmarks : IThroughputSource
    {
        private const int TotalSize = 8 * 1024 * 1024;

        private byte[] _payload;
        private MemoryStream _memorySink;
        private FileStream _nullSink;

        [Params(4 * 1024, 64 * 1024)]
        public int BatchSize { get; set; }

        [Params(StreamTarget.Memory, StreamTarget.Syscall)]
        public StreamTarget Target { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            NativeLibrarySelector.EnsureExpectedLibrary();

            _payload = Datasets.GetPayload(PayloadKind.Blittable, LargeStreamBenchmarks.PayloadSize).AsSpan(0, TotalSize).ToArray();
            _memorySink = new MemoryStream(TotalSize);
            _nullSink = TestStreams.OpenNullSink();
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _nullSink.Dispose();
        }

        [Benchmark(Baseline = true)]
        public long WriteAndFlushBatches_Baseline() => WriteAndFlushBatches(BaselineZstdStream.Compress(GetSink(), CompressionLevel.Optimal, leaveOpen: true));

        [Benchmark]
        public long WriteAndFlushBatches() => WriteAndFlushBatches(ZstdStream.Compress(GetSink(), CompressionLevel.Optimal, leaveOpen: true));

        private Stream GetSink()
        {
            if (Target == StreamTarget.Syscall)
                return _nullSink;

            _memorySink.Position = 0;
            _memorySink.SetLength(0);
            return _memorySink;
        }

        private long WriteAndFlushBatches(Stream zstd)
        {
            using (zstd)
            {
                for (int offset = 0; offset < _payload.Length; offset += BatchSize)
                {
                    zstd.Write(_payload.AsSpan(offset, Math.Min(BatchSize, _payload.Length - offset)));
                    zstd.Flush();
                }
            }

            return _payload.Length;
        }

        public double GetUncompressedBytesPerOperation(string method) => TotalSize;
    }
}
