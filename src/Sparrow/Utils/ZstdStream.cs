using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Utils
{
#if NETCOREAPP3_1_OR_GREATER
    internal sealed class ZstdStream : Stream
    {
        private readonly Stream _inner;
        private readonly bool _compression;
        private readonly bool _leaveOpen;
        private ZstdLib.CompressContext _compressContext;
        private byte[] _tempBuffer = ArrayPool<byte>.Shared.Rent(1024);
        private Memory<byte> _decompressionInput = Memory<byte>.Empty;
        private long _compressedBytesCount;
        private long _uncompressedBytesCount;
        private readonly DisposeLock _disposerLock = new(nameof(ZstdStream));
        private bool _disposed;

        private ZstdStream(Stream inner, bool compression, int level, bool leaveOpen)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _compressContext = new ZstdLib.CompressContext(level);
            _compression = compression;
            _leaveOpen = leaveOpen;
        }

        public static ZstdStream Compress(Stream stream, CompressionLevel compressionLevel = CompressionLevel.Optimal, bool leaveOpen = false) => new(stream, compression: true, ToZstdLevel(compressionLevel), leaveOpen);
        public static ZstdStream Decompress(Stream stream, bool leaveOpen = false) => new(stream, compression: false, 0, leaveOpen);

        public override bool CanRead => _compression == false;
        public override bool CanSeek => false;
        public override bool CanWrite => _compression;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public long CompressedBytesCount { get => _compressedBytesCount; }
        public long UncompressedBytesCount { get => _uncompressedBytesCount; }
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        private unsafe int DecompressStep(ReadOnlySpan<byte> buffer)
        {
            lock (this)
            {
                fixed (byte* pBuffer = buffer, pOutput = _decompressionInput.Span)
                {
                    var output = new ZstdLib.ZSTD_outBuffer { Source = pBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)buffer.Length };
                    var input = new ZstdLib.ZSTD_inBuffer { Source = pOutput, Position = UIntPtr.Zero, Size = (UIntPtr)_decompressionInput.Length };
                    var v = ZstdLib.ZSTD_decompressStream(_compressContext.Decompression, &output, &input);
                    ZstdLib.AssertZstdSuccess(v);
                    _compressedBytesCount += (long)input.Position;
                    _uncompressedBytesCount += (long)output.Position;
                    _decompressionInput = _decompressionInput.Slice((int)input.Position);
                    return (int)output.Position;
                }
            }
        }

        private unsafe (int OutputPosition, int InputPosition, bool Done) CompressStep(ReadOnlySpan<byte> buffer, ZstdLib.ZSTD_EndDirective directive)
        {
            lock (this)
            {
                fixed (byte* pBuffer = buffer, pTempBuffer = _tempBuffer)
                {
                    var input = new ZstdLib.ZSTD_inBuffer { Source = pBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)buffer.Length };
                    var output = new ZstdLib.ZSTD_outBuffer { Source = pTempBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)_tempBuffer.Length };
                    var v = ZstdLib.ZSTD_compressStream2(_compressContext.Compression, &output, &input, directive);
                    ZstdLib.AssertZstdSuccess(v);
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

        public override int Read(byte[] buffer, int offset, int count)
        {
            return Read(new Span<byte>(buffer, offset, count));
        }

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
                        return 0; // nothing left to read

                    _decompressionInput = new Memory<byte>(_tempBuffer, 0, _decompressionInput.Length + read);
                }
            }
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            using (await _disposerLock.EnsureNotDisposedAsync().ConfigureAwait(false))
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
                        return 0; // nothing left to read

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
                    var (outputBytes, inputBytes, _) = CompressStep(buffer, ZstdLib.ZSTD_EndDirective.ZSTD_e_continue);
                    buffer = buffer.Slice(inputBytes);

                    if (outputBytes == 0)
                        continue;

                    _inner.Write(_tempBuffer, 0, outputBytes);
                }
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            Write(new ReadOnlySpan<byte>(buffer, offset, count));
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken).AsTask();
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            using (await _disposerLock.EnsureNotDisposedAsync().ConfigureAwait(false))
            {
                while (buffer.Length > 0)
                {
                    var (outputBytes, inputBytes, _) = CompressStep(buffer.Span, ZstdLib.ZSTD_EndDirective.ZSTD_e_continue);
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
                var (outputBytes, _, _) = CompressStep(ReadOnlySpan<byte>.Empty, ZstdLib.ZSTD_EndDirective.ZSTD_e_flush);
                if (outputBytes == 0)
                    break;
                _inner.Write(_tempBuffer, 0, outputBytes);
            }

            _inner.Flush();
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            using (await _disposerLock.EnsureNotDisposedAsync().ConfigureAwait(false))
            {
                await FlushInternalAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task FlushInternalAsync(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                var (outputBytes, _, _) = CompressStep(ReadOnlySpan<byte>.Empty, ZstdLib.ZSTD_EndDirective.ZSTD_e_flush);
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

                if (_compressContext != null)
                {
                    if (_compression)
                        await FlushInternalAsync().ConfigureAwait(false);

                    while (_compression)
                    {
                        var (outputBytes, _, done) = CompressStep(ReadOnlySpan<byte>.Empty, ZstdLib.ZSTD_EndDirective.ZSTD_e_end);

                        if (done)
                            break;

                        await _inner.WriteAsync(_tempBuffer, 0, outputBytes).ConfigureAwait(false);
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

                if (_compressContext != null)
                {
                    if (_compression)
                        FlushInternal();

                    while (_compression)
                    {
                        var (outputBytes, _, done) = CompressStep(ReadOnlySpan<byte>.Empty, ZstdLib.ZSTD_EndDirective.ZSTD_e_end);

                        if (done)
                            break;

                        _inner.Write(_tempBuffer, 0, outputBytes);
                    }
                }

                if (_leaveOpen == false)
                    _inner.Dispose();

                ReleaseResources();
            }
        }

        private void ReleaseResources()
        {
            lock (this)
            {
                if (_tempBuffer != null)
                {
                    ArrayPool<byte>.Shared.Return(_tempBuffer);
                    _tempBuffer = null;
                }
                _compressContext?.Dispose();
                _compressContext = null;
            }
        }

        private static int ToZstdLevel(CompressionLevel compressionLevel)
        {
            switch (compressionLevel)
            {
                case CompressionLevel.Optimal:
                    return 0;
                case CompressionLevel.Fastest:
                    return 1;
#if NET6_0_OR_GREATER
                case CompressionLevel.SmallestSize:
                    return 22;
#endif
                case CompressionLevel.NoCompression:
                default:
                    throw new ArgumentOutOfRangeException(nameof(compressionLevel), compressionLevel, null);
            }
        }
    }
#endif
}
