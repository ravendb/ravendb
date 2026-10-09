using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Platform;
using Sparrow.Utils;
using Zstd.Benchmark.Infrastructure;

namespace Zstd.Benchmark.Baseline
{
    /// <summary>
    /// Frozen copy of Sparrow.Utils.ZstdStream and ZstdLib.CompressContext as of v7.2 (commit cffe0a99b31), before the zstd
    /// optimizations. Benchmarks run it next to the production ZstdStream in the same process, so a change is measured
    /// against the original under identical conditions instead of across runs. Native calls go to the same libzstd module
    /// the production binding uses. Do not "fix" this class - it is the reference point.
    /// </summary>
    internal sealed class BaselineZstdStream : Stream
    {
        private const int ZSTD_c_compressionLevel = 100;
        private const int ZSTD_c_windowLog = 101;
        private const int ZSTD_e_continue = 0;
        private const int ZSTD_e_flush = 1;
        private const int ZSTD_e_end = 2;
        private const ulong ZSTD_Error_maxCode = unchecked(0UL - 120L);

        private readonly ZstdNativeLibrary _native;
        private readonly Stream _inner;
        private readonly bool _compression;
        private readonly bool _leaveOpen;
        private readonly int _level;
        private unsafe void* _cctx;
        private unsafe void* _dctx;
        private bool _contextReleased;
        private byte[] _tempBuffer = ArrayPool<byte>.Shared.Rent(1024);
        private Memory<byte> _decompressionInput = Memory<byte>.Empty;
        private long _compressedBytesCount;
        private long _uncompressedBytesCount;
        private readonly DisposeLock _disposerLock = new(nameof(BaselineZstdStream));
        private bool _disposed;

        private BaselineZstdStream(Stream inner, bool compression, int level, bool leaveOpen)
        {
            _native = NativeLibrarySelector.Current;
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _level = level;
            _compression = compression;
            _leaveOpen = leaveOpen;
        }

        public static BaselineZstdStream Compress(Stream stream, CompressionLevel compressionLevel = CompressionLevel.Optimal, bool leaveOpen = false) =>
            new(stream, compression: true, ToZstdLevel(compressionLevel), leaveOpen);

        public static BaselineZstdStream Decompress(Stream stream, bool leaveOpen = false) => new(stream, compression: false, 0, leaveOpen);

        public override bool CanRead => _compression == false;
        public override bool CanSeek => false;
        public override bool CanWrite => _compression;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public long CompressedBytesCount => _compressedBytesCount;

        public long UncompressedBytesCount => _uncompressedBytesCount;

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        // ZstdLib.CompressContext.Compression
        private unsafe void* Compression
        {
            get
            {
                if (_cctx != null)
                    return _cctx;

                void* cctx = _native.CreateCCtx();
                if (cctx == null)
                    throw new OutOfMemoryException("Unable to create compression context");

                if (_level > 0)
                    AssertZstdSuccess(_native.CCtxSetParameter(cctx, ZSTD_c_compressionLevel, _level));

                if (PlatformDetails.Is32Bits)
                    AssertZstdSuccess(_native.CCtxSetParameter(cctx, ZSTD_c_windowLog, 16));

                return _cctx = cctx;
            }
        }

        // ZstdLib.CompressContext.Decompression
        private unsafe void* Decompression
        {
            get
            {
                if (_dctx != null)
                    return _dctx;

                void* dctx = _native.CreateDCtx();
                if (dctx == null)
                    throw new OutOfMemoryException("Unable to create compression context");
                return _dctx = dctx;
            }
        }

        private unsafe int DecompressStep(ReadOnlySpan<byte> buffer)
        {
            lock (this)
            {
                fixed (byte* pBuffer = buffer, pOutput = _decompressionInput.Span)
                {
                    ZstdNativeLibrary.StreamBuffer output = new() { Data = pBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)buffer.Length };
                    ZstdNativeLibrary.StreamBuffer input = new() { Data = pOutput, Position = UIntPtr.Zero, Size = (UIntPtr)_decompressionInput.Length };
                    UIntPtr v = _native.DecompressStream(Decompression, &output, &input);
                    AssertZstdSuccess(v);
                    _compressedBytesCount += (long)input.Position;
                    _uncompressedBytesCount += (long)output.Position;
                    _decompressionInput = _decompressionInput.Slice((int)input.Position);
                    return (int)output.Position;
                }
            }
        }

        private unsafe (int OutputPosition, int InputPosition, bool Done) CompressStep(ReadOnlySpan<byte> buffer, int directive)
        {
            lock (this)
            {
                fixed (byte* pBuffer = buffer, pTempBuffer = _tempBuffer)
                {
                    ZstdNativeLibrary.StreamBuffer input = new() { Data = pBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)buffer.Length };
                    ZstdNativeLibrary.StreamBuffer output = new() { Data = pTempBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)_tempBuffer.Length };
                    UIntPtr v = _native.CompressStream2(Compression, &output, &input, directive);
                    AssertZstdSuccess(v);
                    _compressedBytesCount += (long)output.Position;
                    _uncompressedBytesCount += (long)input.Position;
                    return ((int)output.Position, (int)input.Position, v == UIntPtr.Zero);
                }
            }
        }

        private void ShiftBufferData()
        {
            if (_decompressionInput.Length == 0)
                return;

            if (_decompressionInput.Length == _tempBuffer.Length)
                throw new InvalidOperationException("Should never happen, the buffer is full of data that produces not output");

            _decompressionInput.Span.CopyTo(_tempBuffer);
            _decompressionInput = new Memory<byte>(_tempBuffer, 0, _decompressionInput.Length);
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(new Span<byte>(buffer, offset, count));

        public override int Read(Span<byte> buffer)
        {
            using (_disposerLock.EnsureNotDisposed())
            {
                while (true)
                {
                    int read = DecompressStep(buffer);
                    if (read != 0)
                        return read;

                    ShiftBufferData();

                    read = _inner.Read(_tempBuffer, _decompressionInput.Length, _tempBuffer.Length - _decompressionInput.Length);
                    if (read == 0)
                        return 0;

                    _decompressionInput = new Memory<byte>(_tempBuffer, 0, _decompressionInput.Length + read);
                }
            }
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            using (await _disposerLock.EnsureNotDisposedAsync(false).ConfigureAwait(false))
            {
                while (true)
                {
                    int read = DecompressStep(buffer.Span);
                    if (read != 0)
                        return read;

                    ShiftBufferData();

                    read = await _inner.ReadAsync(_tempBuffer, _decompressionInput.Length, _tempBuffer.Length - _decompressionInput.Length, cancellationToken)
                        .ConfigureAwait(false);
                    if (read == 0)
                        return 0;

                    _decompressionInput = new Memory<byte>(_tempBuffer, 0, _decompressionInput.Length + read);
                }
            }
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            using (_disposerLock.EnsureNotDisposed())
            {
                while (buffer.Length > 0)
                {
                    (int outputBytes, int inputBytes, _) = CompressStep(buffer, ZSTD_e_continue);
                    buffer = buffer.Slice(inputBytes);

                    if (outputBytes == 0)
                        continue;

                    _inner.Write(_tempBuffer, 0, outputBytes);
                }
            }
        }

        public override void Write(byte[] buffer, int offset, int count) => Write(new ReadOnlySpan<byte>(buffer, offset, count));

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken).AsTask();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            using (await _disposerLock.EnsureNotDisposedAsync(false).ConfigureAwait(false))
            {
                while (buffer.Length > 0)
                {
                    (int outputBytes, int inputBytes, _) = CompressStep(buffer.Span, ZSTD_e_continue);
                    buffer = buffer.Slice(inputBytes);

                    if (outputBytes == 0)
                        continue;

                    await _inner.WriteAsync(_tempBuffer, 0, outputBytes, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public override void Flush()
        {
            using (_disposerLock.EnsureNotDisposed())
            {
                FlushInternal();
            }
        }

        private void FlushInternal()
        {
            while (true)
            {
                (int outputBytes, _, _) = CompressStep(ReadOnlySpan<byte>.Empty, ZSTD_e_flush);
                if (outputBytes == 0)
                    break;
                _inner.Write(_tempBuffer, 0, outputBytes);
            }

            _inner.Flush();
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            using (await _disposerLock.EnsureNotDisposedAsync(false).ConfigureAwait(false))
            {
                await FlushInternalAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task FlushInternalAsync(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                (int outputBytes, _, _) = CompressStep(ReadOnlySpan<byte>.Empty, ZSTD_e_flush);
                if (outputBytes == 0)
                    break;

                await _inner.WriteAsync(_tempBuffer, 0, outputBytes, cancellationToken).ConfigureAwait(false);
            }

            await _inner.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        public override async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            using (_disposerLock.StartDisposing())
            {
                if (_disposed)
                    return;

                _disposed = true;

                if (_contextReleased == false)
                {
                    if (_compression)
                        await FlushInternalAsync().ConfigureAwait(false);

                    while (_compression)
                    {
                        (int outputBytes, _, bool done) = CompressStep(ReadOnlySpan<byte>.Empty, ZSTD_e_end);

                        await _inner.WriteAsync(_tempBuffer, 0, outputBytes).ConfigureAwait(false);

                        if (done)
                            break;
                    }
                }

                if (_leaveOpen == false)
                    await _inner.DisposeAsync().ConfigureAwait(false);

                ReleaseResources();
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            using (_disposerLock.StartDisposing())
            {
                if (_disposed)
                    return;

                _disposed = true;

                if (_contextReleased == false)
                {
                    if (_compression)
                        FlushInternal();

                    while (_compression)
                    {
                        (int outputBytes, _, bool done) = CompressStep(ReadOnlySpan<byte>.Empty, ZSTD_e_end);

                        _inner.Write(_tempBuffer, 0, outputBytes);

                        if (done)
                            break;
                    }
                }

                if (_leaveOpen == false)
                    _inner.Dispose();

                ReleaseResources();
            }
        }

        private unsafe void ReleaseResources()
        {
            lock (this)
            {
                if (_tempBuffer != null)
                {
                    ArrayPool<byte>.Shared.Return(_tempBuffer);
                    _tempBuffer = null;
                }

                if (_cctx != null)
                {
                    _native.FreeCCtx(_cctx);
                    _cctx = null;
                }

                if (_dctx != null)
                {
                    _native.FreeDCtx(_dctx);
                    _dctx = null;
                }

                _contextReleased = true;
            }
        }

        private static void AssertZstdSuccess(UIntPtr v)
        {
            if (ZSTD_Error_maxCode > v.ToUInt64())
                return;

            throw new InvalidOperationException(NativeLibrarySelector.Current.ErrorName(v));
        }

        private static int ToZstdLevel(CompressionLevel compressionLevel)
        {
            switch (compressionLevel)
            {
                case CompressionLevel.Optimal:
                    return 0;
                case CompressionLevel.Fastest:
                    return 1;
                case CompressionLevel.SmallestSize:
                    return 22;
                default:
                    throw new ArgumentOutOfRangeException(nameof(compressionLevel), compressionLevel, null);
            }
        }
    }
}
