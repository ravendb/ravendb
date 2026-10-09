using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Sparrow.Utils;
using Zstd.Benchmark.Infrastructure;
using NativeMemory = System.Runtime.InteropServices.NativeMemory;

namespace Zstd.Benchmark.Benchmarks
{
    public enum DictionaryCompressionVariant
    {
        /// <summary>What ZstdLib does today: frame header carries the 4-byte dictionary ID.</summary>
        UsingCDict,

        /// <summary>ZSTD_compress_usingCDict_advanced with noDictIDFlag (advanced/deprecated API).</summary>
        UsingCDictAdvancedNoDictId,

        /// <summary>Stable API: ZSTD_CCtx_refCDict + ZSTD_c_dictIDFlag=0 + ZSTD_compress2.</summary>
        RefCDictCompress2NoDictId
    }

    /// <summary>
    /// Candidate ways to stop writing the dictionary ID into every compressed document (Voron stores its own dictionary id).
    /// Raw libzstd calls on the same binary the production binding uses, so variants can be compared before choosing one.
    /// </summary>
    [BenchmarkCategory("DocumentVariants")]
    public unsafe class DocumentCompressionVariantsBenchmarks : IThroughputSource
    {
        private const int ZSTD_c_dictIDFlag = 202;

        [StructLayout(LayoutKind.Sequential)]
        private struct FrameParameters
        {
            public int ContentSizeFlag;
            public int ChecksumFlag;
            public int NoDictIdFlag;
        }

        private delegate* unmanaged[Cdecl]<void*> _createCCtx;
        private delegate* unmanaged[Cdecl]<void*, UIntPtr> _freeCCtx;
        private delegate* unmanaged[Cdecl]<void*, int, int, UIntPtr> _setParameter;
        private delegate* unmanaged[Cdecl]<byte*, UIntPtr, int, void*> _createCDict;
        private delegate* unmanaged[Cdecl]<void*, UIntPtr> _freeCDict;
        private delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, void*, UIntPtr> _compressUsingCDict;
        private delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, void*, FrameParameters, UIntPtr> _compressUsingCDictAdvanced;
        private delegate* unmanaged[Cdecl]<void*, void*, UIntPtr> _refCDict;
        private delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, UIntPtr> _compress2;
        private delegate* unmanaged[Cdecl]<UIntPtr, uint> _isError;

        private void* _cctx;
        private void* _cdict;
        private byte* _input;
        private int[] _offsets;
        private int[] _lengths;
        private byte* _output;
        private int _outputSize;

        public static IEnumerable<string> DatasetNames => new[] { "Orders", "CompanyWithOrders" };

        [ParamsSource(nameof(DatasetNames))]
        public string Dataset { get; set; }

        [Params(DictionaryCompressionVariant.UsingCDict, DictionaryCompressionVariant.UsingCDictAdvancedNoDictId, DictionaryCompressionVariant.RefCDictCompress2NoDictId)]
        public DictionaryCompressionVariant Variant { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            NativeLibrarySelector.EnsureExpectedLibrary();
            IntPtr handle = NativeLibrarySelector.Current.Handle;
            _createCCtx = (delegate* unmanaged[Cdecl]<void*>)NativeLibrary.GetExport(handle, "ZSTD_createCCtx");
            _freeCCtx = (delegate* unmanaged[Cdecl]<void*, UIntPtr>)NativeLibrary.GetExport(handle, "ZSTD_freeCCtx");
            _setParameter = (delegate* unmanaged[Cdecl]<void*, int, int, UIntPtr>)NativeLibrary.GetExport(handle, "ZSTD_CCtx_setParameter");
            _createCDict = (delegate* unmanaged[Cdecl]<byte*, UIntPtr, int, void*>)NativeLibrary.GetExport(handle, "ZSTD_createCDict");
            _freeCDict = (delegate* unmanaged[Cdecl]<void*, UIntPtr>)NativeLibrary.GetExport(handle, "ZSTD_freeCDict");
            _compressUsingCDict = (delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, void*, UIntPtr>)NativeLibrary.GetExport(handle, "ZSTD_compress_usingCDict");
            _compressUsingCDictAdvanced = (delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, void*, FrameParameters, UIntPtr>)NativeLibrary.GetExport(handle, "ZSTD_compress_usingCDict_advanced");
            _refCDict = (delegate* unmanaged[Cdecl]<void*, void*, UIntPtr>)NativeLibrary.GetExport(handle, "ZSTD_CCtx_refCDict");
            _compress2 = (delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, UIntPtr>)NativeLibrary.GetExport(handle, "ZSTD_compress2");
            _isError = (delegate* unmanaged[Cdecl]<UIntPtr, uint>)NativeLibrary.GetExport(handle, "ZSTD_isError");

            DocumentCorpus corpus = Datasets.GetCorpus(Dataset);
            byte[] dictionary = DictionaryTrainer.Train(corpus);
            fixed (byte* p = dictionary)
                _cdict = _createCDict(p, (UIntPtr)dictionary.Length, 3);

            _cctx = _createCCtx();
            if (Variant == DictionaryCompressionVariant.RefCDictCompress2NoDictId)
                Check(_setParameter(_cctx, ZSTD_c_dictIDFlag, 0));

            int count = corpus.Documents.Length;
            _offsets = new int[count];
            _lengths = new int[count];
            _input = (byte*)NativeMemory.Alloc((nuint)corpus.DocumentsBytes);
            int offset = 0, max = 0;
            for (int i = 0; i < count; i++)
            {
                byte[] document = corpus.Documents[i];
                document.CopyTo(new Span<byte>(_input + offset, document.Length));
                _offsets[i] = offset;
                _lengths[i] = document.Length;
                offset += document.Length;
                max = Math.Max(max, document.Length);
            }

            _outputSize = max * 2 + 1024;
            _output = (byte*)NativeMemory.Alloc((nuint)_outputSize);

            // every variant must produce frames production decompression (ZstdLib + digested dictionary) reads back
            using ZstdLib.CompressionDictionary digested = DictionaryTrainer.CreateDictionary(dictionary, id: 1);
            byte[] roundTrip = new byte[max];
            long compressedTotal = 0;
            for (int i = 0; i < count; i++)
            {
                int size = CompressOne(i);
                compressedTotal += size;
                fixed (byte* dst = roundTrip)
                {
                    int decompressed = ZstdLib.Decompress(_output, size, dst, _lengths[i], digested);
                    if (decompressed != _lengths[i] || roundTrip.AsSpan(0, decompressed).SequenceEqual(new ReadOnlySpan<byte>(_input + _offsets[i], decompressed)) == false)
                        throw new InvalidOperationException($"{Variant}: round trip failed for document #{i} of {Dataset}");
                }
            }

            Console.WriteLine($"// [zstd-bench] {Variant} {Dataset}: {(double)compressedTotal / count:N1} compressed bytes per document");
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _freeCCtx(_cctx);
            _freeCDict(_cdict);
            NativeMemory.Free(_input);
            NativeMemory.Free(_output);
        }

        [Benchmark(OperationsPerInvoke = Datasets.DocumentsPerCorpus)]
        public long Compress()
        {
            long total = 0;
            for (int i = 0; i < _lengths.Length; i++)
                total += CompressOne(i);
            return total;
        }

        private int CompressOne(int index)
        {
            byte* src = _input + _offsets[index];
            UIntPtr result;
            switch (Variant)
            {
                case DictionaryCompressionVariant.UsingCDict:
                    result = _compressUsingCDict(_cctx, _output, (UIntPtr)_outputSize, src, (UIntPtr)_lengths[index], _cdict);
                    break;
                case DictionaryCompressionVariant.UsingCDictAdvancedNoDictId:
                    FrameParameters noDictId = new() { ContentSizeFlag = 1, ChecksumFlag = 0, NoDictIdFlag = 1 };
                    result = _compressUsingCDictAdvanced(_cctx, _output, (UIntPtr)_outputSize, src, (UIntPtr)_lengths[index], _cdict, noDictId);
                    break;
                default:
                    // the thread-static production context is shared with the no-dictionary path, so the reference is set on every call
                    _refCDict(_cctx, _cdict);
                    result = _compress2(_cctx, _output, (UIntPtr)_outputSize, src, (UIntPtr)_lengths[index]);
                    break;
            }

            Check(result);
            return (int)result;
        }

        public double GetUncompressedBytesPerOperation(string method) => Datasets.GetCorpus(Dataset).AverageDocumentSize;

        private void Check(UIntPtr code)
        {
            if (_isError(code) != 0)
                throw new InvalidOperationException("zstd error " + code);
        }
    }
}
