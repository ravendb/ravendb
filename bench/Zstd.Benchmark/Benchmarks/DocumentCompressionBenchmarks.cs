using System;
using System.Collections.Generic;
using NativeMemory = System.Runtime.InteropServices.NativeMemory;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Sparrow.Utils;
using Zstd.Benchmark.Infrastructure;

namespace Zstd.Benchmark.Benchmarks
{
    /// <summary>
    /// Voron document compression: one-shot <see cref="ZstdLib.Compress"/> / <see cref="ZstdLib.Decompress"/> per document,
    /// on the thread-static context, with or without a trained dictionary - the same calls TableValueCompressor and Table make.
    /// Reported time is per document.
    /// </summary>
    [BenchmarkCategory("Documents")]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    [CategoriesColumn]
    public unsafe class DocumentCompressionBenchmarks : IThroughputSource
    {
        private const int OverheadSize = 32; // TableValueCompressor.OverheadSize

        private ZstdLib.CompressionDictionary _dictionary;
        private byte* _input;
        private int[] _inputOffsets;
        private int[] _inputLengths;
        private byte* _compressed;
        private int[] _compressedOffsets;
        private int[] _compressedLengths;
        private byte* _scratch;
        private int _scratchSize;
        private void* _baselineContext;
        private ZstdNativeLibrary _native;

        public static IEnumerable<string> DatasetNames => Datasets.DocumentDatasets;

        [ParamsSource(nameof(DatasetNames))]
        public string Dataset { get; set; }

        [Params(false, true)]
        public bool Dictionary { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            NativeLibrarySelector.EnsureExpectedLibrary();

            _native = NativeLibrarySelector.Current;
            _baselineContext = _native.CreateCCtx();

            DocumentCorpus corpus = Datasets.GetCorpus(Dataset);
            _dictionary = Dictionary
                ? DictionaryTrainer.CreateDictionary(DictionaryTrainer.Train(corpus), id: 1)
                : DictionaryTrainer.CreateEmptyDictionary();

            int count = corpus.Documents.Length;
            _inputOffsets = new int[count];
            _inputLengths = new int[count];
            _compressedOffsets = new int[count];
            _compressedLengths = new int[count];

            int maxLength = 0;
            _input = (byte*)NativeMemory.Alloc((nuint)corpus.DocumentsBytes);
            long offset = 0;
            for (int i = 0; i < count; i++)
            {
                byte[] document = corpus.Documents[i];
                document.CopyTo(new Span<byte>(_input + offset, document.Length));
                _inputOffsets[i] = (int)offset;
                _inputLengths[i] = document.Length;
                offset += document.Length;
                maxLength = Math.Max(maxLength, document.Length);
            }

            _scratchSize = ZstdLib.GetMaxCompression(maxLength) + OverheadSize;
            _scratch = (byte*)NativeMemory.Alloc((nuint)Math.Max(_scratchSize, maxLength));

            long compressedCapacity = 0;
            for (int i = 0; i < count; i++)
                compressedCapacity += ZstdLib.GetMaxCompression(_inputLengths[i]);
            _compressed = (byte*)NativeMemory.Alloc((nuint)compressedCapacity);

            offset = 0;
            for (int i = 0; i < count; i++)
            {
                int size = ZstdLib.Compress(_input + _inputOffsets[i], _inputLengths[i], _scratch, _scratchSize, _dictionary);
                Buffer.MemoryCopy(_scratch, _compressed + offset, size, size);
                _compressedOffsets[i] = (int)offset;
                _compressedLengths[i] = size;
                offset += size;

                // correctness check - a broken build or binding must not produce fast but wrong numbers
                int decompressed = ZstdLib.Decompress(_compressed + offset - size, size, _scratch, _inputLengths[i], _dictionary);
                if (decompressed != _inputLengths[i] ||
                    new ReadOnlySpan<byte>(_scratch, decompressed).SequenceEqual(new ReadOnlySpan<byte>(_input + _inputOffsets[i], decompressed)) == false)
                    throw new InvalidOperationException($"Round trip failed for document #{i} of {Dataset}");
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _dictionary?.Dispose();
            _native.FreeCCtx(_baselineContext);
            NativeMemory.Free(_input);
            NativeMemory.Free(_compressed);
            NativeMemory.Free(_scratch);
        }

        /// <summary>
        /// ZstdLib.Compress as of v7.2 (commit cffe0a99b31): ZSTD_compressCCtx at level 3, or ZSTD_compress_usingCDict with the
        /// digested dictionary (frames carry the dictionary id). Runs on its own context, like the thread-static one.
        /// </summary>
        [Benchmark(Baseline = true, OperationsPerInvoke = Datasets.DocumentsPerCorpus), BenchmarkCategory("Compress")]
        public long Compress_Baseline()
        {
            long total = 0;
            for (int i = 0; i < _inputLengths.Length; i++)
            {
                UIntPtr result = _dictionary.Compression == null
                    ? _native.CompressCCtx(_baselineContext, _scratch, (UIntPtr)_scratchSize, _input + _inputOffsets[i], (UIntPtr)_inputLengths[i], 3)
                    : _native.CompressUsingCDict(_baselineContext, _scratch, (UIntPtr)_scratchSize, _input + _inputOffsets[i], (UIntPtr)_inputLengths[i], _dictionary.Compression);
                _native.Check(result);
                total += (long)result;
            }
            return total;
        }

        [Benchmark(OperationsPerInvoke = Datasets.DocumentsPerCorpus), BenchmarkCategory("Compress")]
        public long Compress()
        {
            long total = 0;
            for (int i = 0; i < _inputLengths.Length; i++)
                total += ZstdLib.Compress(_input + _inputOffsets[i], _inputLengths[i], _scratch, _scratchSize, _dictionary);
            return total;
        }

        [Benchmark(OperationsPerInvoke = Datasets.DocumentsPerCorpus), BenchmarkCategory("Decompress")]
        public long Decompress()
        {
            long total = 0;
            for (int i = 0; i < _compressedLengths.Length; i++)
            {
                byte* source = _compressed + _compressedOffsets[i];
                int size = ZstdLib.GetDecompressedSize(source, _compressedLengths[i]);
                total += ZstdLib.Decompress(source, _compressedLengths[i], _scratch, size, _dictionary);
            }
            return total;
        }

        public double GetUncompressedBytesPerOperation(string method) => Datasets.GetCorpus(Dataset).AverageDocumentSize;
    }
}
