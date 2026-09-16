using System;
using System.Linq;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;

namespace Micro.Benchmark.Benchmarks.LZ4
{
    /// <summary>
    /// Measures a full LZ4 encode + decode round trip over two data shapes.
    /// Lz4Benchmark compresses during setup and measures decompression only, so the
    /// encode side is covered here.
    /// </summary>
    [Config(typeof(Lz4RoundTripBenchmark.Config))]
    public unsafe class Lz4RoundTripBenchmark
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job
                {
                    Environment = { Runtime = CoreRuntime.Core10_0, Platform = Platform.X64, Jit = Jit.RyuJit, },
                });

                AddExporter(GetExporters().ToArray());

                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);

                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        public enum DataShape
        {
            /// <summary>
            /// Short random sequences drawn from a small pool and repeated, so the
            /// compressor finds plenty of matches.
            /// </summary>
            HighRepetition,

            /// <summary>
            /// Uniform noise confined to the low four bits of every byte.
            /// </summary>
            LowBitsRandom
        }

        private const int BufferLength = 65 * 1024 * 1024;

        private const int RandomSeed = 1000;

        [Params(DataShape.HighRepetition, DataShape.LowBitsRandom)]
        public DataShape Shape { get; set; }

        private byte[] _input;
        private byte[] _encodedOutput;
        private int _maximumOutputLength;

        [GlobalSetup]
        public void Setup()
        {
            _input = new byte[BufferLength];

            switch (Shape)
            {
                case DataShape.HighRepetition:
                    GenerateHighRepetition(_input);
                    break;

                case DataShape.LowBitsRandom:
                    GenerateLowBitsRandom(_input);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(Shape), Shape, null);
            }

            _maximumOutputLength = (int)Sparrow.Compression.LZ4.MaximumOutputLength(_input.Length);
            _encodedOutput = new byte[_maximumOutputLength];
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _input = null;
            _encodedOutput = null;
        }

        [Benchmark]
        public int RoundTrip()
        {
            fixed (byte* inputPtr = _input)
            fixed (byte* encodedOutputPtr = _encodedOutput)
            {
                int compressedSize = Sparrow.Compression.LZ4.Encode64(inputPtr, encodedOutputPtr, _input.Length, _maximumOutputLength);

                // Decoding back over the input is safe here - the result is identical to
                // what is already there, which is what the original benchmark relied on.
                return Sparrow.Compression.LZ4.Decode64(encodedOutputPtr, compressedSize, inputPtr, _input.Length, true);
            }
        }

        private static void GenerateHighRepetition(byte[] buffer)
        {
            Random main = new Random(RandomSeed);

            int i = 0;
            while (i < buffer.Length)
            {
                int sequenceNumber = main.Next(20);
                int sequenceLength = Math.Min(main.Next(128), buffer.Length - i);

                Random rnd = new Random(sequenceNumber);
                for (int j = 0; j < sequenceLength; j++, i++)
                    buffer[i] = (byte)(rnd.Next() % 255);
            }
        }

        private static void GenerateLowBitsRandom(byte[] buffer)
        {
            const int threshold = 1 << 4;

            Random rnd = new Random(RandomSeed);
            for (int i = 0; i < buffer.Length; i++)
                buffer[i] = (byte)(rnd.Next() % threshold);
        }
    }
}
