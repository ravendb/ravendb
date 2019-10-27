using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;

namespace Raven.Client.Documents.Operations.Attachments
{
    internal class LimitedStream : Stream
    {
        private readonly long _length;
        private readonly ConcatStream _inner;
        private long _read;

        public LimitedStream(ConcatStream inner, long length)
        {
            _inner = inner;
            _length = length;
        }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_inner.Disposed)
                throw new StreamDisposedException($"{nameof(_inner)} stream is disposed.");

            var actualCount = _read + count > _length ? _length - _read : count;
            if (actualCount == 0)
                return 0;

            var read = _inner.Read(buffer, offset, (int)actualCount);
            if (read == 0)
                throw new EndOfStreamException($"You have reached the end of stream before reading whole attachment. _read / _length: {_read} / {_length}, actualCount: {actualCount}.");

            _read += read;
            return read;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (_inner.Disposed)
                throw new StreamDisposedException($"{nameof(_inner)} stream is disposed.");

            var actualCount = _read + count > _length ? _length - _read : count;
            if (actualCount == 0)
                return 0;

            var read = await _inner.ReadAsync(buffer, offset, (int)actualCount, cancellationToken).ConfigureAwait(false);
            if (read == 0)
                throw new EndOfStreamException($"You have reached the end of stream before reading whole attachment. _read / _length: {_read} / {_length}, actualCount: {actualCount}, IsCancellationRequested: {cancellationToken.IsCancellationRequested}");

            _read += read;

            return read;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        internal void ReadToEnd()
        {
            if (_inner.Disposed)
                throw new StreamDisposedException($"{nameof(_inner)} stream is disposed.");

            if (_read == _length)
                return;

            var buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
            try
            {
                while (true)
                {
                    var toRead = Math.Min(buffer.Length, _length - _read);
                    if (toRead == 0)
                        break;

                    var read = _inner.Read(buffer, 0, (int)toRead);
                    if (read == 0)
                        break;

                    _read += read;
                }

                if (_read != _length)
                    throw new EndOfStreamException($"You have reached the end of stream before finishing ReadToEnd. _read / _length: {_read} / {_length}");
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => _length;

        public override long Position
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }
    }
}
