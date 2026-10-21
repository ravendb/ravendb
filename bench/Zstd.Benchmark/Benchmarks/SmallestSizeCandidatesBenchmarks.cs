using System;
using BenchmarkDotNet.Attributes;
using Zstd.Benchmark.Infrastructure;

namespace Zstd.Benchmark.Benchmarks
{
    public enum SmallestSizeCandidate
    {
        /// <summary>What CompressionLevel.SmallestSize maps to today.</summary>
        Level22,

        /// <summary>Level 22 with the window capped at 8MB (match tables shrink with it).</summary>
        Level22Window23,

        Level21,
        Level20,

        /// <summary>The highest level zstd allows without the explicit "ultra" opt-in it requires for 20-22 (memory).</summary>
        Level19
    }

    /// <summary>
    /// Candidates for what CompressionLevel.SmallestSize should map to: level 22 needs a ~870MB compression context and a
    /// ~135MB decompression context per stream. Raw libzstd streaming compression (new context per stream, 32KB writes),
    /// compressed sizes and context sizes are in the report.
    /// </summary>
    [BenchmarkCategory("SmallestSize")]
    [WarmupCount(1)]
    [IterationCount(5)]
    public unsafe class SmallestSizeCandidatesBenchmarks : IThroughputSource
    {
        public const int PayloadSize = 16 * 1024 * 1024;
        private const int ChunkSize = 32 * 1024;

        private ZstdNativeLibrary _native;
        private byte[] _payload;
        private byte[] _output;

        [Params(PayloadKind.Json, PayloadKind.Blittable)]
        public PayloadKind Payload { get; set; }

        [Params(SmallestSizeCandidate.Level22, SmallestSizeCandidate.Level22Window23, SmallestSizeCandidate.Level21, SmallestSizeCandidate.Level20, SmallestSizeCandidate.Level19)]
        public SmallestSizeCandidate Candidate { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            NativeLibrarySelector.EnsureExpectedLibrary();
            _native = NativeLibrarySelector.Current;
            _payload = Datasets.GetPayload(Payload, LargeStreamBenchmarks.PayloadSize).AsSpan(0, PayloadSize).ToArray();
            _output = new byte[_native.CompressBound(PayloadSize)];
        }

        [Benchmark]
        public long Compress() => Compress(_native, _payload, _output, Candidate, workers: 0, out _);

        internal static (int Level, int WindowLog) GetParameters(SmallestSizeCandidate candidate) => candidate switch
        {
            SmallestSizeCandidate.Level22 => (22, 0),
            SmallestSizeCandidate.Level22Window23 => (22, 23),
            SmallestSizeCandidate.Level21 => (21, 0),
            SmallestSizeCandidate.Level20 => (20, 0),
            SmallestSizeCandidate.Level19 => (19, 0),
            _ => throw new ArgumentOutOfRangeException(nameof(candidate))
        };

        /// <summary>
        /// Streaming compression of unknown length (like ZstdStream), returns the compressed size and the context size at the end.
        /// </summary>
        internal static int Compress(ZstdNativeLibrary native, byte[] payload, byte[] output, SmallestSizeCandidate candidate, int workers, out long contextSize)
        {
            (int level, int windowLog) = GetParameters(candidate);
            void* cctx = native.CreateCCtx();
            try
            {
                native.Check(native.CCtxSetParameter(cctx, 100, level));
                if (windowLog > 0)
                    native.Check(native.CCtxSetParameter(cctx, 101, windowLog));
                if (workers > 0)
                    native.Check(native.CCtxSetParameter(cctx, 400, workers));

                fixed (byte* src = payload)
                fixed (byte* dst = output)
                {
                    ZstdNativeLibrary.StreamBuffer outBuffer = new() { Data = dst, Size = (UIntPtr)output.Length };
                    for (int offset = 0; offset < payload.Length; offset += ChunkSize)
                    {
                        ZstdNativeLibrary.StreamBuffer inBuffer = new() { Data = src + offset, Size = (UIntPtr)Math.Min(ChunkSize, payload.Length - offset) };
                        while ((ulong)inBuffer.Position < (ulong)inBuffer.Size)
                            native.Check(native.CompressStream2(cctx, &outBuffer, &inBuffer, 0));
                    }

                    ZstdNativeLibrary.StreamBuffer empty = default;
                    UIntPtr remaining;
                    do
                    {
                        remaining = native.CompressStream2(cctx, &outBuffer, &empty, 2);
                        native.Check(remaining);
                    } while ((ulong)remaining != 0);

                    contextSize = (long)native.SizeOfCCtx(cctx);
                    return (int)outBuffer.Position;
                }
            }
            finally
            {
                native.FreeCCtx(cctx);
            }
        }

        public double GetUncompressedBytesPerOperation(string method) => PayloadSize;
    }
}
