using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security;
using Sparrow.Platform;

namespace Sparrow.Utils
{
#if NETCOREAPP3_1_OR_GREATER
    [SuppressUnmanagedCodeSecurity]
    internal static unsafe class ZstdLib
    {
        private const string LIBZSTD = @"libzstd";

        internal static Func<string, Exception> CreateDictionaryException;

        static ZstdLib()
        {
            CreateDictionaryException = message => new InvalidOperationException(message);
        }

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        private static extern UIntPtr ZSTD_compressBound(UIntPtr srcSize);


        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        private static extern ulong ZSTD_getFrameContentSize(void* src, UIntPtr srcSize);

        const ulong ZSTD_CONTENTSIZE_UNKNOWN = unchecked(0UL - 1);
        const ulong ZSTD_CONTENTSIZE_ERROR = unchecked(0UL - 2);

        public static int GetDecompressedSize(ReadOnlySpan<byte> compressed)
        {
            fixed (byte* srcPtr = compressed)
            {
                return GetDecompressedSize(srcPtr, compressed.Length);
            }
        }

        public static int GetDecompressedSize(byte* srcPtr, int len)
        {
            ulong size = ZSTD_getFrameContentSize(srcPtr, (UIntPtr)len);
            if (size is ZSTD_CONTENTSIZE_ERROR or ZSTD_CONTENTSIZE_UNKNOWN)
                throw new InvalidDataException("Unable to get the content size from ZSTD value");
            return (int)size;
        }

        public static long GetMaxCompression(long size)
        {
            return (long)ZSTD_compressBound((UIntPtr)size);
        }

        public static int GetMaxCompression(int size)
        {
            return (int)ZSTD_compressBound((UIntPtr)size);
        }

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        private static extern void* ZSTD_createCCtx();

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        private static extern UIntPtr ZSTD_CCtx_setParameter(void* cctx, ZSTD_cParameter p, int value);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        private static extern UIntPtr ZSTD_freeCCtx(void* cctx);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        private static extern void* ZSTD_createDCtx();

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        private static extern UIntPtr ZSTD_freeDCtx(void* dctx);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern uint ZSTD_isError(UIntPtr code);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr ZSTD_getErrorName(UIntPtr code);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_compressCCtx(void* ctx, byte* dst, UIntPtr dstCapacity, byte* src, UIntPtr srcSize, int compressionLevel);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_decompressDCtx(void* ctx, byte* dst, UIntPtr dstCapacity, byte* src, UIntPtr srcSize);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_compress_usingCDict(void* ctx, byte* dst, UIntPtr dstCapacity, byte* src, UIntPtr srcSize, void* cdict);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_decompress_usingDDict(void* ctx, byte* dst, UIntPtr dstCapacity, byte* src, UIntPtr srcSize, void* ddict);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* ZSTD_createCDict(byte* dictBuffer, UIntPtr dictSize, int compressionLevel);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_freeCDict(void* CDict);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* ZSTD_createDDict(void* dictBuffer, UIntPtr dictSize);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_freeDDict(void* ddict);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZDICT_trainFromBuffer(byte* dictBuffer, UIntPtr dictBufferCapacity, byte* samplesBuffer, UIntPtr* samplesSizes, uint nbSamples);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_compressStream2(void* ctx, ZSTD_outBuffer* output, ZSTD_inBuffer* input, ZSTD_EndDirective directive);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_decompressStream(void* ctx, ZSTD_outBuffer* output, ZSTD_inBuffer* input);

        internal struct ZSTD_inBuffer
        {
            public void* Source;
            public UIntPtr Size;
            public UIntPtr Position;
        }

        internal struct ZSTD_outBuffer
        {
            public void* Source;
            public UIntPtr Size;
            public UIntPtr Position;
        }

        internal enum ZSTD_EndDirective
        {
            ZSTD_e_continue = 0,
            ZSTD_e_flush = 1,
            ZSTD_e_end = 2
        }

        internal enum ZSTD_cParameter
        {
            ZSTD_c_compressionLevel = 100, /* Set compression parameters according to pre-defined cLevel table.
                              * Note that exact compression parameters are dynamically determined,
                              * depending on both compression level and srcSize (when known).
                              * Default level is ZSTD_CLEVEL_DEFAULT==3.
                              * Special: value 0 means default, which is controlled by ZSTD_CLEVEL_DEFAULT.
                              * Note 1 : it's possible to pass a negative compression level.
                              * Note 2 : setting a level does not automatically set all other compression parameters
                              *   to default. Setting this will however eventually dynamically impact the compression
                              *   parameters which have not been manually set. The manually set
                              *   ones will 'stick'. */
            /* Advanced compression parameters :
             * It's possible to pin down compression parameters to some specific values.
             * In which case, these values are no longer dynamically selected by the compressor */
            ZSTD_c_windowLog = 101,    /* Maximum allowed back-reference distance, expressed as power of 2.
                              * This will set a memory budget for streaming decompression,
                              * with larger values requiring more memory
                              * and typically compressing more.
                              * Must be clamped between ZSTD_WINDOWLOG_MIN and ZSTD_WINDOWLOG_MAX.
                              * Special: value 0 means "use default windowLog".
                              * Note: Using a windowLog greater than ZSTD_WINDOWLOG_LIMIT_DEFAULT
                              *       requires explicitly allowing such size at streaming decompression stage. */
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void AssertZstdSuccess(UIntPtr v)
        {
            ulong code = v.ToUInt64();
            if (ZSTD_Error_maxCode > code)
                return;

            RaiseError(v);
        }

        private static void RaiseError(UIntPtr v)
        {
            throw new InvalidOperationException(Marshal.PtrToStringAnsi(ZSTD_getErrorName(v)));
        }
        const ulong ZSTD_Error_maxCode = unchecked(0UL - 120L);

        private static void AssertSuccess(UIntPtr v, CompressionDictionary dictionary)
        {
            ulong code = v.ToUInt64();
            if (ZSTD_Error_maxCode > code)
                return;

            string ptrToStringAnsi = Marshal.PtrToStringAnsi(ZSTD_getErrorName(v));
            if (dictionary != null)
            {
                throw CreateDictionaryException(ptrToStringAnsi + " on " + dictionary);
            }

            throw CreateDictionaryException(ptrToStringAnsi);
        }

        internal sealed class CompressContext : IDisposable
        {
            private readonly int _level;
            private void* _cctx;
            public void* Compression => _cctx != null ? _cctx : (_cctx = CreateCompression());
            private void* _dctx;
            public void* Decompression => _dctx != null ? _dctx : (_dctx = CreateDecompression());

            public CompressContext(int level)
            {
                _level = level;
            }

            private void* CreateCompression()
            {
                var cctx = ZSTD_createCCtx();
                if (cctx == null)
                {
                    throw new OutOfMemoryException("Unable to create compression context");
                }

                if (_level > 0)
                {
                    var rc = ZSTD_CCtx_setParameter(cctx, ZSTD_cParameter.ZSTD_c_compressionLevel, _level);
                    AssertZstdSuccess(rc);
                }

                if (PlatformDetails.Is32Bits)
                {
                    // set windowLog size to 256KB 
                    var rc = ZSTD_CCtx_setParameter(cctx, ZSTD_cParameter.ZSTD_c_windowLog, 16);
                    AssertZstdSuccess(rc);
                }

                return cctx;
            }

            private void* CreateDecompression()
            {
                var dctx = ZSTD_createDCtx();
                if (dctx == null)
                {
                    throw new OutOfMemoryException("Unable to create compression context");
                }

                return dctx;
            }

            public void Dispose()
            {
                if (_cctx != null)
                {
                    ZSTD_freeCCtx(_cctx);
                    _cctx = null;
                }

                if (_dctx != null)
                {
                    ZSTD_freeDCtx(_dctx);
                    _dctx = null;
                }
                GC.SuppressFinalize(this);
            }

            ~CompressContext()
            {
                Dispose();
            }
        }

        [ThreadStatic]
        private static CompressContext _threadCompressContext;

        public static int Compress(byte* src, int srcLen, byte* dst, int dstLen, CompressionDictionary dictionary)
        {
            _threadCompressContext ??= new CompressContext(level: 0);

            {
                UIntPtr result;

                if (dictionary == null || dictionary.Compression == null)
                {
                    result = ZSTD_compressCCtx(_threadCompressContext.Compression, dst, (UIntPtr)dstLen,
                        src, (UIntPtr)srcLen, 3);
                }
                else
                {
                    result = ZSTD_compress_usingCDict(_threadCompressContext.Compression, dst,
                        (UIntPtr)dstLen, src, (UIntPtr)srcLen, dictionary.Compression);
                }

                AssertSuccess(result, dictionary);
                return (int)result;
            }
        }

        public static int Decompress(ReadOnlySpan<byte> src, Span<byte> dst, CompressionDictionary dictionary)
        {
            fixed (byte* srcPtr = src)
            fixed (byte* dstPtr = dst)
            {
                return Decompress(srcPtr, src.Length, dstPtr, dst.Length, dictionary);
            }
        }

        public static int Decompress( byte* srcPtr, int srcSize, byte* dstPtr, int dstSize, CompressionDictionary dictionary)
        {
            _threadCompressContext ??= new CompressContext(level: 0);

            UIntPtr result;
            if (dictionary == null || dictionary.Compression == null)
            {
                result = ZSTD_decompressDCtx(_threadCompressContext.Decompression, dstPtr, (UIntPtr)dstSize, srcPtr, (UIntPtr)srcSize);
            }
            else
            {
                result = ZSTD_decompress_usingDDict(_threadCompressContext.Decompression, dstPtr, (UIntPtr)dstSize, srcPtr, (UIntPtr)srcSize, dictionary.Decompression);
            }

            AssertSuccess(result, dictionary);
            return (int)result;
        }

        internal sealed class CompressionDictionary : IDisposable
        {
            public void* Compression;
            public void* Decompression;
            public int Id;
            public byte ExpectedCompressionRatio;

#if DEBUG
            public string DictionaryHash;
#endif

            public CompressionDictionary(int id, byte* buffer, int size, int compressionLevel)
            {
                Id = id;
                if (buffer == null)
                {
#if DEBUG
                    DictionaryHash = "<null>";
#endif
                    return;
                }

#if DEBUG
                var hash = new byte[32];
                fixed (byte* pHash = hash)
                {
                    Sodium.crypto_generichash(pHash, (UIntPtr)32, buffer, (ulong)size, null, UIntPtr.Zero);
                }
                DictionaryHash = Convert.ToBase64String(hash);
#endif

                Compression = ZSTD_createCDict(buffer, (UIntPtr)size, compressionLevel);
                Decompression = ZSTD_createDDict(buffer, (UIntPtr)size);
                if (Compression == null || Decompression == null)
                {
                    Dispose();
                    throw new OutOfMemoryException("Unable to allocate memory for dictionary");
                }
            }

            public override string ToString()
            {
                return "Dictionary #" + Id
#if DEBUG
                     + " - " + DictionaryHash
#endif

;
            }

            ~CompressionDictionary()
            {
#if DEBUG
                Trace.WriteLine("CompressionDictionary finalized without being properly disposed");
#endif
                Dispose();
            }

            public void Dispose()
            {
                GC.SuppressFinalize(this);
                if (Compression != null)
                {
                    ZSTD_freeCDict(Compression);
                    Compression = null;
                }

                if (Decompression != null)
                {
                    ZSTD_freeDDict(Decompression);
                    Decompression = null;
                }
            }
        }

        public static void Train(ReadOnlySpan<byte> plainTextBuffer, ReadOnlySpan<UIntPtr> sizes, ref Span<byte> output)
        {
            fixed (byte* textPtr = plainTextBuffer)
            fixed (byte* outputPtr = output)
            fixed (UIntPtr* sizesPtr = sizes)
            {
                var len = ZDICT_trainFromBuffer(outputPtr, (UIntPtr)output.Length, textPtr, sizesPtr, (uint)sizes.Length);
                if (ZSTD_isError(len) != 0)
                {
                    string ptrToStringAnsi = Marshal.PtrToStringAnsi(ZSTD_getErrorName(len));
                    throw new InvalidOperationException(
                        $"Unable to train dictionary with {sizes.Length} [{string.Join(", ", sizes.ToArray())}] results: {ptrToStringAnsi}");
                }

                output = output.Slice(0, (int)len);
            }
        }
    }
#endif
}
