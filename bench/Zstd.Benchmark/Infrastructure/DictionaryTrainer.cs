using System;
using System.Collections.Generic;
using Sparrow.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Global;

namespace Zstd.Benchmark.Infrastructure
{
    /// <summary>
    /// Mirrors TableValueCompressor.MaybeTrainCompressionDictionary: walk the most recent documents backwards, skip documents
    /// larger than 32KB, stop at 256 samples or 1MB, require at least 16 samples, and train into a single Voron page.
    /// </summary>
    internal static unsafe class DictionaryTrainer
    {
        public const int MaxSamples = 256;
        private const int MaxSampleSize = 32 * 1024;
        private const int MaxTotalSize = 1024 * 1024;
        private const int MinSamples = 16;

        public static readonly int DictionaryCapacity = Constants.Storage.PageSize - PageHeader.SizeOf - sizeof(CompressionDictionaryInfo);

        public static (byte[] Samples, UIntPtr[] Sizes) CollectSamples(byte[][] trainingDocuments)
        {
            List<byte[]> picked = new();
            int totalSize = 0;
            for (int i = trainingDocuments.Length - 1; i >= 0 && picked.Count < MaxSamples && totalSize < MaxTotalSize; i--)
            {
                byte[] document = trainingDocuments[i];
                if (document.Length > MaxSampleSize)
                    continue;

                picked.Add(document);
                totalSize += document.Length;
            }

            if (picked.Count < MinSamples)
                throw new InvalidOperationException($"Only {picked.Count} samples, production would not train a dictionary");

            byte[] samples = new byte[totalSize];
            UIntPtr[] sizes = new UIntPtr[picked.Count];
            int offset = 0;
            for (int i = 0; i < picked.Count; i++)
            {
                picked[i].CopyTo(samples, offset);
                sizes[i] = (UIntPtr)picked[i].Length;
                offset += picked[i].Length;
            }

            return (samples, sizes);
        }

        /// <summary>
        /// Returns the trained dictionary bytes (what Voron stores in the dictionaries tree).
        /// </summary>
        public static byte[] Train(byte[] samples, UIntPtr[] sizes)
        {
            byte[] buffer = new byte[DictionaryCapacity];
            Span<byte> output = buffer;
            ZstdLib.Train(samples, sizes, ref output);
            return output.ToArray();
        }

        public static byte[] Train(DocumentCorpus corpus)
        {
            (byte[] samples, UIntPtr[] sizes) = CollectSamples(corpus.TrainingDocuments);
            return Train(samples, sizes);
        }

        /// <summary>
        /// Production creates digested dictionaries with compression level 3, see TableValueCompressor and Table.CreateCompressionDictionary.
        /// </summary>
        public static ZstdLib.CompressionDictionary CreateDictionary(byte[] dictionary, int id)
        {
            fixed (byte* p = dictionary)
                return new ZstdLib.CompressionDictionary(id, p, dictionary.Length, 3);
        }

        /// <summary>
        /// What production uses for tables without a trained dictionary yet (dictionary id 0).
        /// </summary>
        public static ZstdLib.CompressionDictionary CreateEmptyDictionary() => new(0, null, 0, 3);
    }
}
