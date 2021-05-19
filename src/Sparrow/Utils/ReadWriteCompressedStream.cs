using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Threading;

namespace Sparrow.Utils
{
    internal class ReadWriteCompressedStream : Stream
    {
        private readonly Stream _inner;
        private readonly ZstdStream _input, _output;
        private readonly DisposeOnceAsync<SingleAttempt> _dispose;

        public ReadWriteCompressedStream(Stream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _input = ZstdStream.Decompress(inner);
            _output = ZstdStream.Compress(inner);
            _dispose = new DisposeOnceAsync<SingleAttempt>(DisposeInternal);
        }

        public unsafe ReadWriteCompressedStream(Stream inner, JsonOperationContext.MemoryBuffer alreadyOnBuffer)
        {
            Stream innerInput = inner;
            int valid = alreadyOnBuffer.Valid - alreadyOnBuffer.Used;
            if (valid > 0)
            {
                byte[] buffer = ArrayPool<byte>.Shared.Rent(valid);
                fixed (byte* pBuffer = buffer)
                {
                    Memory.Copy(pBuffer, alreadyOnBuffer.Address + alreadyOnBuffer.Used, valid);
                }

                innerInput = new ConcatStream(new ConcatStream.RentedBuffer { Buffer = buffer, Offset = 0, Count = valid }, inner);
                alreadyOnBuffer.Valid = alreadyOnBuffer.Used = 0; // consume all the data from the buffer
            }

            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _input = ZstdStream.Decompress(inner);
            _output = ZstdStream.Compress(inner);
            _dispose = new DisposeOnceAsync<SingleAttempt>(DisposeInternal);
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanTimeout => _inner.CanTimeout;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }
        public override int ReadTimeout
        {
            get => _inner.ReadTimeout; 
            set => _inner.ReadTimeout = value;
        }
        public override int WriteTimeout
        {
            get => _inner.WriteTimeout; 
            set => _inner.WriteTimeout = value;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback? callback, object? state)
        {
            return _input.BeginRead(buffer, offset, count, callback, state);
        }

        public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback? callback, object? state)
        {
            return _output.BeginWrite(buffer, offset, count, callback, state);
        }

        public override void CopyTo(Stream destination, int bufferSize)
        {
            _input.CopyTo(destination, bufferSize);
        }

        public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            return _input.CopyToAsync(destination, bufferSize, cancellationToken);
        }

        public override int EndRead(IAsyncResult asyncResult)
        {
            return _input.EndRead(asyncResult);
        }

        public override void EndWrite(IAsyncResult asyncResult)
        {
            _output.EndWrite(asyncResult);
        }

        public override int ReadByte()
        {
            return _input.ReadByte();
        }

        public override void WriteByte(byte value)
        {
            _output.WriteByte(value);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _input.Read(buffer, offset, count);
        }

        public override int Read(Span<byte> buffer)
        {
            return _input.Read(buffer);
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = new CancellationToken())
        {
            return await _input.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return await _input.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return _output.WriteAsync(buffer, offset, count, cancellationToken);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _output.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            _output.Write(buffer);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = new CancellationToken())
        {
            return _output.WriteAsync(buffer, cancellationToken);
        }

        public override void Flush()
        {
            _output.Flush();
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return _output.FlushAsync(cancellationToken);
        }

        private async Task DisposeInternal()
        {
            if (_output != null)
                await _output.DisposeAsync().ConfigureAwait(false);

            await _inner.DisposeAsync().ConfigureAwait(false);

            if (_input != null)
                await _input.DisposeAsync().ConfigureAwait(false);
        }

        protected override void Dispose(bool disposing)
        {
            _dispose.DisposeAsync().GetAwaiter().GetResult();
        }

        public override async ValueTask DisposeAsync()
        {
            await _dispose.DisposeAsync().ConfigureAwait(false);
        }
    }
}
