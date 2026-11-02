using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Zstd.Benchmark.Infrastructure
{
    /// <summary>
    /// Direct handle to one libzstd binary, independent of the production binding in <c>Sparrow.Utils.ZstdLib</c>.
    /// Used to identify a binary (version, multithreading support) and for raw sweeps the production binding cannot express
    /// (e.g. negative compression levels).
    /// </summary>
    internal sealed unsafe class ZstdNativeLibrary
    {
        private const int ZSTD_c_compressionLevel = 100;
        private const int ZSTD_c_nbWorkers = 400;

        private readonly delegate* unmanaged[Cdecl]<sbyte*> _versionString;
        private readonly delegate* unmanaged[Cdecl]<int> _minCLevel;
        private readonly delegate* unmanaged[Cdecl]<int> _maxCLevel;
        private readonly delegate* unmanaged[Cdecl]<void*> _createCCtx;
        private readonly delegate* unmanaged[Cdecl]<void*, UIntPtr> _freeCCtx;
        private readonly delegate* unmanaged[Cdecl]<void*, int, int, UIntPtr> _setParameter;
        private readonly delegate* unmanaged[Cdecl]<UIntPtr, uint> _isError;
        private readonly delegate* unmanaged[Cdecl]<UIntPtr, sbyte*> _getErrorName;
        private readonly delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, UIntPtr> _compress2;
        private readonly delegate* unmanaged[Cdecl]<UIntPtr, UIntPtr> _compressBound;

        public readonly delegate* unmanaged[Cdecl]<void*> CreateCCtx;
        public readonly delegate* unmanaged[Cdecl]<void*, UIntPtr> FreeCCtx;
        public readonly delegate* unmanaged[Cdecl]<void*> CreateDCtx;
        public readonly delegate* unmanaged[Cdecl]<void*, UIntPtr> FreeDCtx;
        public readonly delegate* unmanaged[Cdecl]<void*, int, int, UIntPtr> CCtxSetParameter;
        public readonly delegate* unmanaged[Cdecl]<void*, StreamBuffer*, StreamBuffer*, int, UIntPtr> CompressStream2;
        public readonly delegate* unmanaged[Cdecl]<void*, StreamBuffer*, StreamBuffer*, UIntPtr> DecompressStream;
        public readonly delegate* unmanaged[Cdecl]<void*, UIntPtr> SizeOfCCtx;
        public readonly delegate* unmanaged[Cdecl]<void*, UIntPtr> SizeOfDCtx;
        public readonly delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, int, UIntPtr> CompressCCtx;
        public readonly delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, void*, UIntPtr> CompressUsingCDict;

        /// <summary>
        /// ZSTD_inBuffer / ZSTD_outBuffer (identical layouts).
        /// </summary>
        public struct StreamBuffer
        {
            public void* Data;
            public UIntPtr Size;
            public UIntPtr Position;
        }

        public ZstdNativeLibrary(string path)
        {
            Path = System.IO.Path.GetFullPath(path);
            if (File.Exists(Path) == false)
                throw new FileNotFoundException($"libzstd binary not found: {Path}", Path);

            Handle = NativeLibrary.Load(Path);
            _versionString = (delegate* unmanaged[Cdecl]<sbyte*>)NativeLibrary.GetExport(Handle, "ZSTD_versionString");
            _minCLevel = (delegate* unmanaged[Cdecl]<int>)NativeLibrary.GetExport(Handle, "ZSTD_minCLevel");
            _maxCLevel = (delegate* unmanaged[Cdecl]<int>)NativeLibrary.GetExport(Handle, "ZSTD_maxCLevel");
            _createCCtx = (delegate* unmanaged[Cdecl]<void*>)NativeLibrary.GetExport(Handle, "ZSTD_createCCtx");
            _freeCCtx = (delegate* unmanaged[Cdecl]<void*, UIntPtr>)NativeLibrary.GetExport(Handle, "ZSTD_freeCCtx");
            _setParameter = (delegate* unmanaged[Cdecl]<void*, int, int, UIntPtr>)NativeLibrary.GetExport(Handle, "ZSTD_CCtx_setParameter");
            _isError = (delegate* unmanaged[Cdecl]<UIntPtr, uint>)NativeLibrary.GetExport(Handle, "ZSTD_isError");
            _getErrorName = (delegate* unmanaged[Cdecl]<UIntPtr, sbyte*>)NativeLibrary.GetExport(Handle, "ZSTD_getErrorName");
            _compress2 = (delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, UIntPtr>)NativeLibrary.GetExport(Handle, "ZSTD_compress2");
            _compressBound = (delegate* unmanaged[Cdecl]<UIntPtr, UIntPtr>)NativeLibrary.GetExport(Handle, "ZSTD_compressBound");

            CreateCCtx = _createCCtx;
            FreeCCtx = _freeCCtx;
            CCtxSetParameter = _setParameter;
            CreateDCtx = (delegate* unmanaged[Cdecl]<void*>)NativeLibrary.GetExport(Handle, "ZSTD_createDCtx");
            FreeDCtx = (delegate* unmanaged[Cdecl]<void*, UIntPtr>)NativeLibrary.GetExport(Handle, "ZSTD_freeDCtx");
            CompressStream2 = (delegate* unmanaged[Cdecl]<void*, StreamBuffer*, StreamBuffer*, int, UIntPtr>)NativeLibrary.GetExport(Handle, "ZSTD_compressStream2");
            DecompressStream = (delegate* unmanaged[Cdecl]<void*, StreamBuffer*, StreamBuffer*, UIntPtr>)NativeLibrary.GetExport(Handle, "ZSTD_decompressStream");
            SizeOfCCtx = (delegate* unmanaged[Cdecl]<void*, UIntPtr>)NativeLibrary.GetExport(Handle, "ZSTD_sizeof_CCtx");
            SizeOfDCtx = (delegate* unmanaged[Cdecl]<void*, UIntPtr>)NativeLibrary.GetExport(Handle, "ZSTD_sizeof_DCtx");
            CompressCCtx = (delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, int, UIntPtr>)NativeLibrary.GetExport(Handle, "ZSTD_compressCCtx");
            CompressUsingCDict = (delegate* unmanaged[Cdecl]<void*, byte*, UIntPtr, byte*, UIntPtr, void*, UIntPtr>)NativeLibrary.GetExport(Handle, "ZSTD_compress_usingCDict");
        }

        public string Path { get; }

        public IntPtr Handle { get; }

        public string Version => new string(_versionString());

        public int MinLevel => _minCLevel();

        public int MaxLevel => _maxCLevel();

        public bool SupportsMultithreading
        {
            get
            {
                void* cctx = _createCCtx();
                try
                {
                    return _isError(_setParameter(cctx, ZSTD_c_nbWorkers, 1)) == 0;
                }
                finally
                {
                    _freeCCtx(cctx);
                }
            }
        }

        public string Describe() => $"libzstd {Version} (levels {MinLevel}..{MaxLevel}, multithreading: {(SupportsMultithreading ? "yes" : "no")}) from {Path}";

        public int CompressBound(int size) => (int)_compressBound((UIntPtr)size);

        /// <summary>
        /// One-shot compression at an arbitrary level (including negative "fast" levels), optionally with worker threads.
        /// </summary>
        public int Compress(ReadOnlySpan<byte> source, Span<byte> destination, int level, int workers = 0)
        {
            void* cctx = _createCCtx();
            try
            {
                Check(_setParameter(cctx, ZSTD_c_compressionLevel, level));
                if (workers > 0)
                    Check(_setParameter(cctx, ZSTD_c_nbWorkers, workers));

                fixed (byte* src = source)
                fixed (byte* dst = destination)
                {
                    UIntPtr result = _compress2(cctx, dst, (UIntPtr)destination.Length, src, (UIntPtr)source.Length);
                    Check(result);
                    return (int)result;
                }
            }
            finally
            {
                _freeCCtx(cctx);
            }
        }

        public string ErrorName(UIntPtr code) => new string(_getErrorName(code));

        public void Check(UIntPtr code)
        {
            if (_isError(code) != 0)
                throw new InvalidOperationException(new string(_getErrorName(code)));
        }
    }
}
