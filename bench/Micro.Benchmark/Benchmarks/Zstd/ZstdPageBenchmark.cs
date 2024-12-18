using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Global;
using Platform = BenchmarkDotNet.Environments.Platform;

namespace Micro.Benchmark.Benchmarks.LZ4
{
    [Config(typeof(Config))]
    public unsafe class ZstdPageBenchmark
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job(RunMode.Default)
                {
                    Environment =
                    {
                        Runtime = CoreRuntime.Core70,
                        Platform = Platform.X64,
                        Jit = Jit.RyuJit
                    }
                });

                // Exporters for data
                AddExporter(GetExporters().ToArray());

                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);

                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        private const int NumberOfOperations = 10000;

        public int RandomSeed { get; set; } = 1337;

        /// <summary>
        /// Number of precomputed sequences to use when generating data
        /// </summary>
        public int NumberOfSequences { get; set; } = 100;

        /// <summary>
        /// Maximum length of a sequence piece. Sequence length will be
        /// uniformly distributed between 1 and this length.
        /// </summary>
        public int GeneratedSequenceMaximumLength { get; set; } = 64;


        // Notice that length of actual values will be uniformly distributed
        // between 0 and MaximumLength. Hence, we have that the average array
        // length will be MaximumLength/2.
        [Params(1024, 2048, 4096, 4096 * 4, 4096 * 16)]
        public int DataMaximumLength { get; set; } = 1024;

        /// <summary>
        /// Probability of using a predefined sequence when generating data.
        /// </summary>
        public double SequenceUsageProbability { get; set; } = 0.9;

        /// <summary>
        /// Probability of repeating any given sequence when generating data
        /// </summary>
        public double SequenceRepetitionProbability { get; set; } = 0.7;

        /// <summary>
        /// Probability of flipping one or more bytes in a sequence when
        /// generating data
        /// </summary>
        public double DataFlipProbability { get; set; } = 0.1;

        /// <summary>
        /// Probability that current byte will be flipped when flipping
        /// a sequence in data generation
        /// </summary>
        public double DataByteFlipProbability { get; set; } = 0.1;

        private ByteStringContext _allocator;
        private List<Tuple<ByteString, int>> _lz4Buffers;
        private ByteString _lz4Buffer;

        private List<Tuple<ByteString, int>> _zstdBuffers;
        private ByteString _zstdBuffer;
        private ByteString _zstdDictionaryStorage;
        private ZstdLib.CompressionDictionary _zstdDictionary;

        [GlobalSetup]
        public void Setup()
        {
            var generator = new Random(RandomSeed);
            _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            _lz4Buffers = new List<Tuple<ByteString, int>>();
            _zstdBuffers = new List<Tuple<ByteString, int>>();

            _allocator.Allocate(Constants.Storage.PageSize, out _zstdDictionaryStorage);

            // Generate the precomputed sequences to be used when generating data.
            var sequences = new List<byte[]>();
            var trainBuffer = new byte[NumberOfSequences * DataMaximumLength];

            Span<UIntPtr> sized = new UIntPtr[NumberOfSequences];

            var start = 0;
            for (int i = 0; i < NumberOfSequences; i++)
            {
                int length = generator.Next(1, GeneratedSequenceMaximumLength);
                var sequence = new byte[length];
                generator.NextBytes(sequence);
                sequences.Add(sequence);

                sequence.CopyTo(trainBuffer.AsSpan(start, length));
                start += length;
                sized[i] = (uint)length;
            }

            Span<byte> dictionarySpan = _zstdDictionaryStorage.ToSpan();
            ZstdLib.Train(trainBuffer, sized, ref dictionarySpan);
            _zstdDictionary = new ZstdLib.CompressionDictionary(1, _zstdDictionaryStorage.Ptr, dictionarySpan.Length, 3);

            // Compute the length of the maximum output data. This is an upper bound
            // to be able to always use the same buffer for decompression.
            _allocator.Allocate(Sparrow.Compression.LZ4.MaximumOutputLength(DataMaximumLength), out _lz4Buffer);
            _allocator.Allocate(ZstdLib.GetMaxCompression(DataMaximumLength), out _zstdBuffer);

            int lz4CompressedSize = 0;
            int zstdCompressedSize = 0;

            var buffer = new byte[DataMaximumLength];
            for (int i = 0; i < NumberOfOperations; i++)
            {
                var usedSequences = new List<int>();
                for (var j = 0; j < DataMaximumLength; j++)
                {
                    bool useSequence = generator.NextDouble() < SequenceUsageProbability;
                    if (sequences.Count > 0 && useSequence)
                    {
                        byte[] sequence;
                        bool repeatSequence = generator.NextDouble() < SequenceRepetitionProbability;
                        if (repeatSequence && usedSequences.Count > 0)
                        {
                            int index = generator.Next(usedSequences.Count);
                            sequence = sequences[usedSequences[index]];
                        }
                        else
                        {
                            int index = generator.Next(sequences.Count);
                            sequence = sequences[index];
                            usedSequences.Add(index);
                        }

                        fixed (byte* bufferPtr = &buffer[j])
                        fixed (byte* sequencePtr = sequence)
                        {
                            int amount = Math.Min(sequence.Length, DataMaximumLength - j);
                            Memory.Copy(bufferPtr, sequencePtr, amount);
                            j += amount;
                        }
                    }
                    else
                    {
                        var spontaneousSequenceLength = Math.Min(generator.Next(GeneratedSequenceMaximumLength), DataMaximumLength - j);
                        for (int k = 0; k < spontaneousSequenceLength; k++, j++)
                        {
                            buffer[j] = (byte)generator.Next(256);
                        }
                    }
                }

                // Flip bytes on the generated sequence, as required
                bool flipGeneratedSequence = generator.NextDouble() < DataFlipProbability;
                if (flipGeneratedSequence)
                {
                    for (var j = 0; j < DataMaximumLength; j++)
                    {
                        bool flipGeneratedByte = generator.NextDouble() < DataByteFlipProbability;
                        if (flipGeneratedByte)
                            buffer[j] ^= (byte)generator.Next(256);
                    }
                }

                // Calculate compression size and store the generated data
                fixed (byte* bufferPtr = buffer)
                {
                    int compressedSize = Sparrow.Compression.LZ4.Encode64(bufferPtr, _lz4Buffer.Ptr, DataMaximumLength, _lz4Buffer.Length);
                    _allocator.From(_lz4Buffer.Ptr, compressedSize, ByteStringType.Immutable, out ByteString unmanagedBuffer);
                    _lz4Buffers.Add(new Tuple<ByteString, int>(unmanagedBuffer, DataMaximumLength));
                    lz4CompressedSize += compressedSize;

                    compressedSize = ZstdLib.Compress(bufferPtr, DataMaximumLength, _zstdBuffer.Ptr, _zstdBuffer.Length, _zstdDictionary);
                    _allocator.From(_zstdBuffer.Ptr, compressedSize, ByteStringType.Immutable, out unmanagedBuffer);
                    _zstdBuffers.Add(new Tuple<ByteString, int>(unmanagedBuffer, DataMaximumLength));
                    zstdCompressedSize += compressedSize;
                }
            }

            Console.WriteLine($"LZ4 Compressed Size: {lz4CompressedSize}");
            Console.WriteLine($"Zstd Compressed Size: {zstdCompressedSize}");
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _lz4Buffers.Clear();
            _zstdBuffers.Clear();
            _allocator.Dispose();
        }

        [Benchmark(Baseline=true, OperationsPerInvoke = NumberOfOperations)]
        public void Lz4()
        {
            foreach (var tuple in _lz4Buffers)
            {
                Sparrow.Compression.LZ4.Decode64(tuple.Item1.Ptr, tuple.Item1.Length, _lz4Buffer.Ptr, tuple.Item2, true);
            }
        }

        [Benchmark(OperationsPerInvoke = NumberOfOperations)]
        public void Zstd()
        {
            foreach (var tuple in _zstdBuffers)
            {
                ZstdLib.Decompress(tuple.Item1.Ptr, tuple.Item1.Length, _zstdBuffer.Ptr, _zstdBuffer.Length, _zstdDictionary);
            }
        }
    }
}
