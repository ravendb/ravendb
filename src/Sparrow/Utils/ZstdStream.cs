using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Threading;

namespace Sparrow.Utils
{
#if NETCOREAPP3_1_OR_GREATER
    internal class ZstdStream : Stream
    {
        private readonly Stream _inner;
        private readonly bool _compression;
        private ZstdLib.CompressContext _compressContext = new ZstdLib.CompressContext();
        private byte[] _tempBuffer = ArrayPool<byte>.Shared.Rent(1024);
        private Memory<byte> _decompressionInput = Memory<byte>.Empty;
        private readonly DisposeOnce<SingleAttempt> _disposeOnce;
        private long _compressedBytesCount;
        private long _uncompressedBytesCount;

        private ZstdStream(Stream inner, bool compression)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _compression = compression;
            _disposeOnce = new DisposeOnce<SingleAttempt>(DisposeInternal);
        }

        public static ZstdStream Compress(Stream stream) => new ZstdStream(stream, compression: true);
        public static ZstdStream Decompress(Stream stream) => new ZstdStream(stream, compression: false);

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
            lock(this)
            {
                if (_compressContext == null)
                    throw new ObjectDisposedException("_compressContext already disposed");

                fixed (byte* pBuffer = buffer, pOutput = _decompressionInput.Span)
                {
                    var output = new ZstdLib.ZSTD_outBuffer {Source = pBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)buffer.Length};
                    var input = new ZstdLib.ZSTD_inBuffer {Source = pOutput, Position = UIntPtr.Zero, Size = (UIntPtr)_decompressionInput.Length};
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
            lock(this)
            {
                if (_compressContext == null)
                    throw new ObjectDisposedException("_compressContext already disposed");

                fixed (byte* pBuffer = buffer, pTempBuffer = _tempBuffer)
                {
                    var input = new ZstdLib.ZSTD_inBuffer {Source = pBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)buffer.Length};
                    var output = new ZstdLib.ZSTD_outBuffer {Source = pTempBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)_tempBuffer.Length};
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
            if (_disposeOnce.Disposed)
                throw new ObjectDisposedException("Object was already disposed!");

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

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_disposeOnce.Disposed)
                throw new ObjectDisposedException("Object was already disposed!");

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

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            if (_disposeOnce.Disposed)
                throw new ObjectDisposedException("Object was already disposed!");

            while (buffer.Length > 0)
            {
                var (outputBytes, inputBytes, _) = CompressStep(buffer, ZstdLib.ZSTD_EndDirective.ZSTD_e_continue);
                buffer = buffer.Slice(inputBytes);
                
                if (outputBytes == 0)
                    continue;
                
                _inner.Write(_tempBuffer, 0, outputBytes);
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
            if (_disposeOnce.Disposed)
                throw new ObjectDisposedException("Object was already disposed!");

            while (buffer.Length > 0)
            {
                var (outputBytes, inputBytes, _) = CompressStep(buffer.Span, ZstdLib.ZSTD_EndDirective.ZSTD_e_continue);
                buffer = buffer.Slice(inputBytes);
                
                if (outputBytes == 0)
                    continue;

                await _inner.WriteAsync(_tempBuffer, 0, outputBytes, cancellationToken).ConfigureAwait(false);
            }
        }

        public override void Flush()
        {
            if (_disposeOnce.Disposed)
                throw new ObjectDisposedException("Object was already disposed!");

            while (true)
            {
                var (outputBytes, _, _) = CompressStep(ReadOnlySpan<byte>.Empty, ZstdLib.ZSTD_EndDirective.ZSTD_e_flush);
                if (outputBytes == 0)
                    break;
                _inner.Write(_tempBuffer, 0, outputBytes);
            }
            _inner?.Flush();
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            if (_disposeOnce.Disposed)
                throw new ObjectDisposedException("Object was already disposed!");

            while (true)
            {
                var (outputBytes, _, _) = CompressStep(ReadOnlySpan<byte>.Empty, ZstdLib.ZSTD_EndDirective.ZSTD_e_flush);
                if (outputBytes == 0)
                    break;

                await _inner.WriteAsync(_tempBuffer, 0, outputBytes, cancellationToken).ConfigureAwait(false);
            }
            await _inner.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        private void DisposeInternal()
        {
            if (_compressContext != null)
            {
                while (_compression)
                {
                    var (outputBytes, _, done) = CompressStep(ReadOnlySpan<byte>.Empty, ZstdLib.ZSTD_EndDirective.ZSTD_e_end);
                   
                    if (done)
                        break;

                    _inner.Write(_tempBuffer, 0, outputBytes);
                }
            }
            ReleaseResources();
        }

        protected override void Dispose(bool disposing)
        {
            _disposeOnce.Dispose();
        }

        private void ReleaseResources()
        {
            lock(this)
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
    }
#endif
}
