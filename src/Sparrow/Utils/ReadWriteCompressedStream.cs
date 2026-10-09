using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Threading;

namespace Sparrow.Utils
{
#if NETCOREAPP3_1_OR_GREATER
    internal sealed class ReadWriteCompressedStream : Stream
    {
        // the transport: written to directly, provides the timeouts
        private readonly Stream _inner;
        // what the decompressing side reads: the transport, possibly preceded by bytes already read from it; disposing it disposes the transport
        private readonly Stream _innerInput;
        private readonly ZstdStream _input, _output;
        private readonly DisposeOnce<SingleAttempt> _dispose;

        public ReadWriteCompressedStream(Stream inner)
        {
            _inner = _innerInput = inner ?? throw new ArgumentNullException(nameof(inner));
            _input = ZstdStream.Decompress(inner, leaveOpen: true);
            _output = ZstdStream.Compress(inner, leaveOpen: true);
            _dispose = new DisposeOnce<SingleAttempt>(DisposeInternal);
        }

        /// <param name="inner">The transport.</param>
        /// <param name="alreadyOnBuffer">Bytes read from the transport into this buffer but not consumed yet (e.g. read ahead while parsing the
        /// connection negotiation) belong to the compressed stream, so they are decompressed first.</param>
        public unsafe ReadWriteCompressedStream(Stream inner, JsonOperationContext.MemoryBuffer alreadyOnBuffer)
        {
            if (inner == null)
                throw new ArgumentNullException(nameof(inner));

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

            _inner = inner;
            _innerInput = innerInput;
            _input = ZstdStream.Decompress(innerInput, leaveOpen: true);
            _output = ZstdStream.Compress(inner, leaveOpen: true);
            _dispose = new DisposeOnce<SingleAttempt>(DisposeInternal);
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

        internal sealed class TotalBytes
        {
            public long Compressed { get; set; }
            public long Uncompressed { get; set; }
        }

        public TotalBytes GetTotalBytesSent()
        {
            return new TotalBytes
            {
                Compressed = _output.CompressedBytesCount,
                Uncompressed = _output.UncompressedBytesCount,
            };
        }

        public TotalBytes GetTotalBytesReceived()
        {
            return new TotalBytes
            {
                Compressed = _input.CompressedBytesCount,
                Uncompressed = _input.UncompressedBytesCount,
            };
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            return _input.BeginRead(buffer, offset, count, callback, state);
        }

        public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
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

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = new CancellationToken())
        {
            return _input.ReadAsync(buffer, cancellationToken);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return _input.ReadAsync(buffer, offset, count, cancellationToken);
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

        private bool _disposing;
        private void DisposeInternal()
        {
            base.Dispose(_disposing);

            // Callers are expected to call Flush() before Dispose(), if they don't, that is on them.
            // We first dispose the inner stream, (typically a network one), so any pending operations on that will
            // fail, and then we dispose the compression streams, without flushing, so we won't try to write to the
            // inner stream (which was already disposed).
            try
            {
                _innerInput?.Dispose();
            }
            finally
            {
                // We explicitly dispose the inner stream first, so we cannot flush
                _output?.DisposeWithoutFlushing();
                _input?.Dispose();
            }
        }

        protected override void Dispose(bool disposing)
        {
            _disposing = disposing;
            _dispose.Dispose();
        }
    }
#endif
}
