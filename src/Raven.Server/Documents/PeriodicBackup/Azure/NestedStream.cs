using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.PeriodicBackup.Azure
{
    internal class NestedStream : Stream
    {
        private Stream _innerStream;

        private readonly long _length;

        private long _remainingBytes;

        public NestedStream(Stream innerStream, long length)
        {
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));

            _innerStream = innerStream ?? throw new ArgumentNullException(nameof(innerStream));
            _remainingBytes = length;
            _length = length;
        }

        public override bool CanRead => _innerStream.CanRead;

        public override bool CanSeek => _innerStream.CanSeek;

        public override bool CanWrite => false;

        public override long Length
        {
            get
            {
                CheckDisposed();

                return _innerStream.CanSeek ?
                    _length : throw new NotSupportedException();
            }
        }

        public override long Position
        {
            get
            {
                CheckDisposed();
                return _length - _remainingBytes;
            }

            set => Seek(value, SeekOrigin.Begin);
        }

        public override void Flush() => throw new NotSupportedException();

        public override Task FlushAsync(CancellationToken cancellationToken) => throw new NotSupportedException();

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            CheckDisposed();

            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            if (offset < 0 || count < 0)
                throw new ArgumentOutOfRangeException();

            if (offset + count > buffer.Length)
                throw new ArgumentException();

            count = (int)Math.Min(count, _remainingBytes);

            if (count <= 0)
                return 0;

            var bytesRead = await _innerStream.ReadAsync(buffer, offset, count, cancellationToken);
            _remainingBytes -= bytesRead;
            return bytesRead;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            CheckDisposed();

            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            if (offset < 0 || count < 0)
                throw new ArgumentOutOfRangeException();

            if (offset + count > buffer.Length)
                throw new ArgumentException();

            count = (int)Math.Min(count, _remainingBytes);

            if (count <= 0)
                return 0;

            var bytesRead = _innerStream.Read(buffer, offset, count);
            _remainingBytes -= bytesRead;
            return bytesRead;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            CheckDisposed();

            if (_remainingBytes < 0)
                return 0;

            buffer = buffer.Slice(0, (int)Math.Min(buffer.Length, _remainingBytes));

            if (buffer.IsEmpty)
                return 0;

            var bytesRead = await _innerStream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            _remainingBytes -= bytesRead;
            return bytesRead;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            CheckDisposed();

            if (CanSeek == false)
                throw new NotSupportedException("Stream is not seekable");

            var newOffset = origin switch
            {
                SeekOrigin.Current => offset,
                SeekOrigin.End => _length + offset - Position,
                SeekOrigin.Begin => offset - Position,
                _ => throw new ArgumentOutOfRangeException(nameof(origin))
            };

            if (Position + newOffset < 0)
                throw new IOException("Cannot seek before beginning of the stream");

            var currentPosition = _innerStream.Position;
            var newPosition = _innerStream.Seek(newOffset, SeekOrigin.Current);
            _remainingBytes -= newPosition - currentPosition;
            return Position;
        }

        public override void SetLength(long value) => throw new NotSupportedException();

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            CheckDisposed();
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            CheckDisposed();
            throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing == false)
                return;

            // the caller is responsible for disposing the base stream
            _innerStream = null;
        }

        private void CheckDisposed()
        {
            if (_innerStream == null)
                throw new ObjectDisposedException(GetType().Name);
        }
    }
}
