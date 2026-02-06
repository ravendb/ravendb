using System;
using System.Linq;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Compression;

namespace Micro.Benchmark.Benchmarks.LZ4
{
    /// <summary>
    /// Side-by-side comparison of the optimized LZ4 vs the reference (pre-optimization) implementation.
    /// Measures both compression and decompression throughput for small data sizes.
    /// </summary>
    [Config(typeof(Config))]
    public unsafe class LZ4ComparisonBenchmark
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job
                {
                    Environment =
                    {
                        Runtime = CoreRuntime.Core10_0,
                        Platform = Platform.X64,
                        Jit = Jit.RyuJit
                    }
                });

                AddExporter(GetExporters().ToArray());
                AddColumn(StatisticColumn.AllStatistics);
                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);
                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        [Params(16, 256, 4096)]
        public int DataSize { get; set; }

        [Params("sequential", "highly_compressible", "mixed")]
        public string Pattern { get; set; }

        private byte[] _inputData;
        private byte[] _outputBuffer;

        // Pre-compressed by each implementation (they may produce different compressed output)
        private byte[] _compressedByOptimized;
        private byte[] _compressedByReference;
        private int _compressedSizeOptimized;
        private int _compressedSizeReference;

        [GlobalSetup]
        public void Setup()
        {
            _inputData = GenerateTestData(DataSize, Pattern);

            int maxCompressedSize = Sparrow.Compression.LZ4.MaximumOutputLength(DataSize);
            _compressedByOptimized = new byte[maxCompressedSize];
            _compressedByReference = new byte[maxCompressedSize];
            _outputBuffer = new byte[DataSize];

            fixed (byte* inputPtr = _inputData)
            fixed (byte* compOptPtr = _compressedByOptimized)
            fixed (byte* compRefPtr = _compressedByReference)
            {
                _compressedSizeOptimized = Sparrow.Compression.LZ4.Encode64(inputPtr, compOptPtr, DataSize, maxCompressedSize);
                _compressedSizeReference = LZ4Reference.Encode64(inputPtr, compRefPtr, DataSize, maxCompressedSize);
            }
        }

        [Benchmark(Baseline = true)]
        public int Compress_Reference()
        {
            fixed (byte* inputPtr = _inputData)
            fixed (byte* outputPtr = _outputBuffer)
            {
                return LZ4Reference.Encode64(inputPtr, outputPtr, DataSize, _outputBuffer.Length);
            }
        }

        [Benchmark]
        public int Compress_Optimized()
        {
            fixed (byte* inputPtr = _inputData)
            fixed (byte* outputPtr = _outputBuffer)
            {
                return Sparrow.Compression.LZ4.Encode64(inputPtr, outputPtr, DataSize, _outputBuffer.Length);
            }
        }

        [Benchmark]
        public int Decompress_Reference()
        {
            fixed (byte* compressedPtr = _compressedByReference)
            fixed (byte* outputPtr = _outputBuffer)
            {
                return LZ4Reference.Decode64(compressedPtr, _compressedSizeReference, outputPtr, DataSize, true);
            }
        }

        [Benchmark]
        public int Decompress_Optimized()
        {
            fixed (byte* compressedPtr = _compressedByOptimized)
            fixed (byte* outputPtr = _outputBuffer)
            {
                return Sparrow.Compression.LZ4.Decode64(compressedPtr, _compressedSizeOptimized, outputPtr, DataSize, true);
            }
        }

        private static byte[] GenerateTestData(int size, string pattern)
        {
            var data = new byte[size];
            var rng = new Random(42);

            switch (pattern)
            {
                case "sequential":
                    for (int i = 0; i < size; i++)
                        data[i] = (byte)(i & 0xFF);
                    break;

                case "highly_compressible":
                    for (int i = 0; i < size; i++)
                        data[i] = (byte)(i % 4);
                    break;

                case "mixed":
                    int pos = 0;
                    while (pos < size)
                    {
                        int chunkSize = Math.Min(rng.Next(8, 64), size - pos);
                        bool usePattern = rng.NextDouble() < 0.6;

                        if (usePattern)
                        {
                            byte patternByte = (byte)rng.Next(32, 127);
                            for (int i = 0; i < chunkSize && pos < size; i++, pos++)
                                data[pos] = patternByte;
                        }
                        else
                        {
                            for (int i = 0; i < chunkSize && pos < size; i++, pos++)
                                data[pos] = (byte)rng.Next(256);
                        }
                    }
                    break;

                default:
                    throw new ArgumentException($"Unknown pattern: {pattern}");
            }

            return data;
        }
    }
}
