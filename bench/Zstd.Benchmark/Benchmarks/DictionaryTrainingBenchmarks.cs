using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using Sparrow.Utils;
using Zstd.Benchmark.Infrastructure;

namespace Zstd.Benchmark.Benchmarks
{
    /// <summary>
    /// Cost of (re)training a collection's compression dictionary, which TableValueCompressor does inside a write transaction
    /// whenever compression ratio degrades: ZDICT training plus digesting the result into CDict/DDict.
    /// </summary>
    [BenchmarkCategory("Training")]
    public class DictionaryTrainingBenchmarks : IThroughputSource
    {
        private byte[] _samples;
        private UIntPtr[] _sizes;

        public static IEnumerable<string> DatasetNames => Datasets.DocumentDatasets;

        [ParamsSource(nameof(DatasetNames))]
        public string Dataset { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            NativeLibrarySelector.EnsureExpectedLibrary();
            (_samples, _sizes) = DictionaryTrainer.CollectSamples(Datasets.GetCorpus(Dataset).TrainingDocuments);
        }

        [Benchmark]
        public int TrainAndDigest()
        {
            byte[] dictionary = DictionaryTrainer.Train(_samples, _sizes);
            using ZstdLib.CompressionDictionary digested = DictionaryTrainer.CreateDictionary(dictionary, id: 1);
            return dictionary.Length;
        }

        public double GetUncompressedBytesPerOperation(string method)
        {
            (byte[] samples, _) = DictionaryTrainer.CollectSamples(Datasets.GetCorpus(Dataset).TrainingDocuments);
            return samples.Length;
        }
    }
}
