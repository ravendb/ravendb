using System;
using System.Linq;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
namespace Micro.Benchmark.Benchmarks.LZ4
{
    /// <summary>
    /// Benchmark for LZ4 compression/decompression focusing on small data sizes
    /// typical in RavenDB (128 bytes to several KB). Used to track performance
    /// improvements from RavenDB-9187 LZ4 optimizations.
    /// </summary>
    [Config(typeof(Config))]
    public unsafe class LZ4SmallDataBenchmark
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

        // Small data sizes typical in RavenDB
        [Params(128, 256, 512, 1024, 2048, 4096, 8192)]
        public int DataSize { get; set; }

        // Different data patterns to test various compression scenarios
        [Params("sequential", "highly_compressible", "mixed")]
        public string Pattern { get; set; }

        private byte[] _inputData;
        private byte[] _compressedData;
        private byte[] _outputBuffer;
        private int _compressedSize;

        [GlobalSetup]
        public void Setup()
        {
            _inputData = GenerateTestData(DataSize, Pattern);

            int maxCompressedSize = Sparrow.Compression.LZ4.MaximumOutputLength(DataSize);
            _compressedData = new byte[maxCompressedSize];
            _outputBuffer = new byte[DataSize];

            // Pre-compress for decompression benchmark
            fixed (byte* inputPtr = _inputData)
            fixed (byte* compressedPtr = _compressedData)
            {
                _compressedSize = Sparrow.Compression.LZ4.Encode64(inputPtr, compressedPtr, DataSize, maxCompressedSize);
            }
        }

        [Benchmark]
        public int Compress()
        {
            fixed (byte* inputPtr = _inputData)
            fixed (byte* outputPtr = _outputBuffer)
            {
                return Sparrow.Compression.LZ4.Encode64(inputPtr, outputPtr, DataSize, _outputBuffer.Length);
            }
        }

        [Benchmark]
        public int Decompress()
        {
            fixed (byte* compressedPtr = _compressedData)
            fixed (byte* outputPtr = _outputBuffer)
            {
                return Sparrow.Compression.LZ4.Decode64(compressedPtr, _compressedSize, outputPtr, DataSize, true);
            }
        }

        private static byte[] GenerateTestData(int size, string pattern)
        {
            var data = new byte[size];
            var rng = new Random(42); // Seeded for reproducibility

            switch (pattern)
            {
                case "sequential":
                    // Sequential bytes 0,1,2,3... - moderately compressible
                    for (int i = 0; i < size; i++)
                        data[i] = (byte)(i & 0xFF);
                    break;

                case "highly_compressible":
                    // Repeated patterns - very compressible (common in JSON)
                    for (int i = 0; i < size; i++)
                        data[i] = (byte)(i % 4);
                    break;

                case "mixed":
                    // Mix of patterns simulating real JSON documents
                    int pos = 0;
                    while (pos < size)
                    {
                        // Simulate JSON structure with repeated patterns and variable data
                        int chunkSize = Math.Min(rng.Next(8, 64), size - pos);
                        bool usePattern = rng.NextDouble() < 0.6;

                        if (usePattern)
                        {
                            // Repeated pattern (like JSON keys/structure)
                            byte patternByte = (byte)rng.Next(32, 127);
                            for (int i = 0; i < chunkSize && pos < size; i++, pos++)
                                data[pos] = patternByte;
                        }
                        else
                        {
                            // Variable data (like JSON values)
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
