using System;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow.Server;

namespace Voron.Data.Tables
{
    public static unsafe class ZstdLib
    {
        private const string LIBZSTD = @"libzstd";

        static ZstdLib()
        {
            DynamicNativeLibraryResolver.Register(typeof(ZstdLib).Assembly,LIBZSTD);
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
                ulong size = ZSTD_getFrameContentSize(srcPtr, (UIntPtr)compressed.Length);
                if (size == ZSTD_CONTENTSIZE_ERROR || size == ZSTD_CONTENTSIZE_UNKNOWN)
                    throw new InvalidDataException("Unable to get the content size from ZSTD value");

                return (int)size;
            }
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
        public static extern UIntPtr ZSTD_compress_usingCDict(void* ctx, byte* dst, UIntPtr dstCapacity, byte* src, UIntPtr srcSize, void* cdict);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_decompress_usingDDict(void* ctx, byte* dst, UIntPtr dstCapacity, byte* src, UIntPtr srcSize, void * ddict);
        
        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* ZSTD_createCDict(byte* dictBuffer, UIntPtr dictSize, int compressionLevel);
        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_freeCDict(void* CDict);
        
        
        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* ZSTD_createDDict(void* dictBuffer, UIntPtr dictSize);
        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr   ZSTD_freeDDict(void* ddict);

        [DllImport(LIBZSTD, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr  ZDICT_trainFromBuffer(byte* dictBuffer, UIntPtr dictBufferCapacity, byte* samplesBuffer, UIntPtr* samplesSizes, uint nbSamples);
        
        private static void AssertSuccess(UIntPtr v, CompressionDictionary dictionary)
        {
            if (ZSTD_isError(v) == 0) 
                return;

            string ptrToStringAnsi = Marshal.PtrToStringAnsi(ZSTD_getErrorName(v));
            if (dictionary != null)
            {
                string hash = Convert.ToBase64String(dictionary.Hash.AsSpan());
                throw new InvalidOperationException(ptrToStringAnsi + " on dictionary " + hash);
            }
            throw new InvalidOperationException(ptrToStringAnsi);
        }

        private class CompressContext
        {
            public void* Compression;
            public void* Decompression;

            public CompressContext()
            {
                Compression = ZSTD_createCCtx();
                if (Compression == null)
                {
                    GC.SuppressFinalize(this);
                    throw new OutOfMemoryException("Unable to create compression context");
                }
                Decompression = ZSTD_createDCtx();
                if (Decompression == null)
                {
                    ZSTD_freeCCtx(Compression);
                    GC.SuppressFinalize(this);
                    Compression = null;
                    throw new OutOfMemoryException("Unable to create compression context");
                }

            }

            ~CompressContext()
            {
                if (Compression != null)
                {
                    ZSTD_freeCCtx(Compression);
                    Compression = null;
                }

                if (Decompression != null)
                {
                    ZSTD_freeDCtx(Decompression);
                    Decompression = null;
                }
            }
        }

        [ThreadStatic]
        private static CompressContext _threadCompressContext;

        public static int Compress(ReadOnlySpan<byte> src, Span<byte> dst, CompressionDictionary dictionary)
        {
            if(_threadCompressContext == null)
                _threadCompressContext = new CompressContext();

            fixed (byte* srcPtr = src)
            fixed (byte* dstPtr = dst)
            {
                var result = ZSTD_compress_usingCDict(_threadCompressContext.Compression, dstPtr, (UIntPtr)dst.Length, srcPtr, (UIntPtr)src.Length, dictionary.Compression);
                AssertSuccess(result, dictionary);
                return (int)result;
            }
        }

        public static int Decompress(ReadOnlySpan<byte> src, Span<byte> dst, CompressionDictionary dictionary)
        {
            if (_threadCompressContext == null)
                _threadCompressContext = new CompressContext();

            fixed (byte* srcPtr = src)
            fixed (byte* dstPtr = dst)
            {
                var result = ZSTD_decompress_usingDDict(_threadCompressContext.Decompression, dstPtr, (UIntPtr)dst.Length, srcPtr, (UIntPtr)src.Length, dictionary.Decompression);
                AssertSuccess(result, dictionary);
                return (int)result;
            }
        }

        public class CompressionDictionary : IDisposable
        {
            public void* Compression;
            public void* Decompression;
            public Slice Hash;
            public string HashBase64;
            public byte ExpectedCompressionRatio;

            public CompressionDictionary(Slice hash, byte* buffer, int size, int compressionLevel)
            {
                Hash = hash;
                HashBase64 = Convert.ToBase64String(hash.AsSpan());
                if (buffer != null)
                {
                    Compression = ZSTD_createCDict(buffer, (UIntPtr)size, compressionLevel);
                    Decompression = ZSTD_createDDict(buffer, (UIntPtr)size);
                    if (Compression == null || Decompression == null)
                    {
                        Dispose();
                        throw new OutOfMemoryException("Unable to allocate memory fro dictionary");
                    }
                }
            }

            public override string ToString()
            {
                return HashBase64;
            }

            ~CompressionDictionary()
            {
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
            fixed(byte* outputPtr = output)
            fixed(UIntPtr* sizesPtr = sizes )
            {
                var len = ZDICT_trainFromBuffer(outputPtr, (UIntPtr)output.Length, textPtr, sizesPtr, (uint)sizes.Length);
                AssertSuccess(len, null);
                output = output.Slice(0, (int)len);
            }
        }
    }
}
