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
        private Stream _inner;
        private readonly bool _compression;
        private ZstdLib.CompressContext _compressContext = new ZstdLib.CompressContext();
        private byte[] _tempBuffer = ArrayPool<byte>.Shared.Rent(1024);
        private Memory<byte> _decompressionInput = Memory<byte>.Empty;
        private readonly DisposeOnceAsync<SingleAttempt> _disposeOnce;
        private readonly SemaphoreSlim _readSemaphore = new SemaphoreSlim(1);
        private readonly SemaphoreSlim _writeSemaphore = new SemaphoreSlim(1);
        private ZstdStream(Stream inner, bool compression)
        {
            _inner = inner;
            _compression = compression;
            _disposeOnce = new DisposeOnceAsync<SingleAttempt>(DisposeInternal);
        }

        public static ZstdStream Compress(Stream stream) => new ZstdStream(stream, compression: true);
        public static ZstdStream Decompress(Stream stream) => new ZstdStream(stream, compression: false);

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

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
            fixed (byte* pBuffer = buffer, pOutput = _decompressionInput.Span)
            {
                var output = new ZstdLib.ZSTD_outBuffer { Source = pBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)buffer.Length };
                var input = new ZstdLib.ZSTD_inBuffer { Source = pOutput, Position = UIntPtr.Zero, Size = (UIntPtr)_decompressionInput.Length };
                var v = ZstdLib.ZSTD_decompressStream(_compressContext.Streaming, &output, &input);
                ZstdLib.AssertZstdSuccess(v);
                _decompressionInput = _decompressionInput.Slice((int)input.Position);
                return (int)output.Position;
            }
        }

        private unsafe (int OutputPosition, int InputPosition, bool Done) CompressStep(ReadOnlySpan<byte> buffer, ZstdLib.ZSTD_EndDirective directive)
        {
            fixed (byte* pBuffer = buffer, pTempBuffer = _tempBuffer)
            {
                var input = new ZstdLib.ZSTD_inBuffer { Source = pBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)buffer.Length };
                var output = new ZstdLib.ZSTD_outBuffer { Source = pTempBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)_tempBuffer.Length };
                var v = ZstdLib.ZSTD_compressStream2(_compressContext.Compression, &output, &input, directive);
                ZstdLib.AssertZstdSuccess(v);
                return ((int)output.Position, (int)input.Position, v == UIntPtr.Zero);
            }
        }

        private unsafe void ShiftBufferData()
        {
            if (_decompressionInput.Length == 0)
                return;

            if (_decompressionInput.Length == _tempBuffer.Length)
                throw new InvalidOperationException("Should never happen, the buffer is full of data that produces not output");

            fixed (byte* pTempBuf = _tempBuffer, pCurVal = _decompressionInput.Span)
            {
                if (pTempBuf == pCurVal)
                    return;
                Buffer.MemoryCopy(pCurVal, pTempBuf, _tempBuffer.Length, _decompressionInput.Length);
                _decompressionInput = new Memory<byte>(_tempBuffer, 0, _decompressionInput.Length);
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return Read(new Span<byte>(buffer, offset, count));
        }

        public override int Read(Span<byte> buffer)
        {
            if (_readSemaphore.Wait(10_000) == false)
                throw new InvalidOperationException("Couldn't acquire write lock for 10 seconds!");

            try
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
                    read = DecompressStep(buffer);
                    if (read != 0)
                        return read;
                }
            }
            catch (ObjectDisposedException)
            {
                // object was already disposed, so nothing to do...
            }
            finally { _readSemaphore.Release(); }

            return 0;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await _readSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
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
                    read = DecompressStep(buffer.Span);
                    if (read != 0)
                        return read;
                }
            }
            catch (ObjectDisposedException)
            {
                // object was already disposed, so nothing to do...
            }
            finally
            {
                _readSemaphore.Release();
            }
            return 0;
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            if (_writeSemaphore.Wait(10_000) == false)
                throw new InvalidOperationException("Couldn't acquire write lock for 10 seconds!");

            try
            {
                if (_disposeOnce.Disposed)
                    throw new ObjectDisposedException("Object was already disposed!");

                while (buffer.Length > 0)
                {
                    var (outputBytes, inputBytes, _) = CompressStep(buffer, ZstdLib.ZSTD_EndDirective.ZSTD_e_continue);
                    buffer = buffer.Slice(inputBytes);
                    _inner.Write(_tempBuffer, 0, outputBytes);
                }
            }
            catch (ObjectDisposedException)
            {
                // object was already disposed, so nothing to do...
            }
            finally
            {
                _writeSemaphore.Release();
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
            await _writeSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                if (_disposeOnce.Disposed)
                    throw new ObjectDisposedException("Object was already disposed!");

                while (buffer.Length > 0)
                {
                    var (outputBytes, inputBytes, _) = CompressStep(buffer.Span, ZstdLib.ZSTD_EndDirective.ZSTD_e_continue);
                    buffer = buffer.Slice(inputBytes);
                    await _inner.WriteAsync(_tempBuffer, 0, outputBytes, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (ObjectDisposedException)
            {
                // object was already disposed, so nothing to do...
            }
            finally
            {
                _writeSemaphore.Release();
            }
        }

        public override void Flush()
        {
            if (_writeSemaphore.Wait(10_000) == false)
                throw new InvalidOperationException("Couldn't acquire write lock for 10 seconds!");

            try
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

                _inner.Flush();
            }
            catch (ObjectDisposedException)
            {
                // object was already disposed, so nothing to do...
            }
            finally
            {
                _writeSemaphore.Release();
            }
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            await _writeSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
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
            catch (ObjectDisposedException)
            {
                // object was already disposed, so nothing to do...
            }
            finally
            {
                _writeSemaphore.Release();
            }
        }

        private async Task DisposeInternal()
        {
            try
            {
                if (_compression == false)
                {
                    await _readSemaphore.WaitAsync().ConfigureAwait(false);
                }
                else
                {
                    await _writeSemaphore.WaitAsync().ConfigureAwait(false);

                    if (_compressContext != null)
                    {
                        while (_compression && _inner != null)
                        {
                            var (outputBytes, _, done) = CompressStep(ReadOnlySpan<byte>.Empty, ZstdLib.ZSTD_EndDirective.ZSTD_e_end);

                            if (done)
                                break;

                            await _inner.WriteAsync(_tempBuffer, 0, outputBytes).ConfigureAwait(false);
                        }
                    }
                }
                ReleaseResources();
            }
            finally
            {
                try
                {
                    if (_compression)
                        _writeSemaphore.Release();
                    else
                        _readSemaphore.Release();
                }
                catch (ObjectDisposedException)
                {
                    // if this happens, this means that the current instance of semaphore has already been disposed, so don't care...
                }
            }
        }

        public override async ValueTask DisposeAsync()
        {
            await _disposeOnce.DisposeAsync().ConfigureAwait(false);
        }

        protected override void Dispose(bool disposing)
        {
            _disposeOnce.DisposeAsync().GetAwaiter().GetResult();
        }

        private void ReleaseResources()
        {
            _inner = null;
            _compressContext?.Dispose();
            _compressContext = null;
            if (_tempBuffer != null)
            {
                ArrayPool<byte>.Shared.Return(_tempBuffer);
                _tempBuffer = null;
            }
        }
    }
#endif
}
