using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Operations.Attachments
{
    internal sealed class LimitedStream : Stream
    {
        public long OverallRead { get; private set; }
        private readonly long _length;
        private readonly long _currentPos;
        private readonly Stream _inner;
        private long _read;
        private bool _disposed;
        internal IDisposable _disposable;

        public LimitedStream(Stream inner, long length, long currentPos, long overallRead)
        {
            OverallRead = overallRead;
            _inner = inner;
            _length = length;
            _currentPos = currentPos;
        }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override long Seek(long position, SeekOrigin origin)
        {
            if (position > _length)
                throw new ArgumentOutOfRangeException(nameof(position));

            var offset = _read - position;
            _read -= offset;
            OverallRead -= offset;

            return _inner.Seek(_currentPos + _read, SeekOrigin.Begin);
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_disposed)
                ThrowDisposedException();

            if (OverallRead < _currentPos)
                ReadToEnd();

            var actualCount = _read + count > _length ? _length - _read : count;
            if (actualCount == 0)
                return 0;

            var read = _inner.Read(buffer, offset, (int)actualCount);
            if (read == 0)
                ThrowEndOfStreamException((int)actualCount);

            _read += read;
            OverallRead += read;
            return read;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (_disposed)
                ThrowDisposedException();

            if (OverallRead < _currentPos)
                await ReadToEndAsync(cancellationToken).ConfigureAwait(false);

            var actualCount = _read + count > _length ? _length - _read : count;
            if (actualCount == 0)
                return 0;

            var read = await _inner.ReadAsync(buffer, offset, (int)actualCount, cancellationToken).ConfigureAwait(false);
            if (read == 0)
                ThrowEndOfStreamException((int)actualCount, cancellationToken);

            _read += read;
            OverallRead += read;
            return read;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            _disposed = true;
            _disposable?.Dispose();
        }

        private void ReadToEnd()
        {
            var buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
            try
            {
                while (true)
                {
                    var toRead = Math.Min(buffer.Length, _currentPos - OverallRead);
                    if (toRead == 0)
                        break;

                    var read = _inner.Read(buffer, 0, (int)toRead);
                    if (read == 0)
                        break;

                    OverallRead += read;
                }

                if (_currentPos != OverallRead)
                    ThrowEndOfStreamException();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private async Task ReadToEndAsync(CancellationToken ct)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
            try
            {
                while (true)
                {
                    var toRead = Math.Min(buffer.Length, _currentPos - OverallRead);
                    if (toRead == 0)
                        break;

                    var read = await _inner.ReadAsync(buffer, 0, (int)toRead, ct).ConfigureAwait(false);
                    if (read == 0)
                        break;

                    OverallRead += read;
                }

                if (_currentPos != OverallRead)
                    ThrowEndOfStreamException();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private static void ThrowDisposedException()
        {
            throw new ObjectDisposedException("Stream was already disposed");
        }

        private void ThrowEndOfStreamException(int? count = null, CancellationToken? cancellationToken = null)
        {
            var msg1 = count == null ? string.Empty : $", actualCount: {count}";
            var msg2 = cancellationToken == null ? string.Empty : $"IsCancellationRequested: {cancellationToken.Value.IsCancellationRequested}";
            var msg = $"You have reached the end of stream before finishing ReadToEnd. _read / _length: {_read} / {_length}, OverallRead / _currentPos: {OverallRead} / {_currentPos}{msg1}{msg2}";

            throw new EndOfStreamException(msg);
        }

        public override bool CanRead => _inner.CanRead;

        public override bool CanSeek => _inner.CanSeek;

        public override bool CanWrite => false;

        public override long Length => _length;

        public override long Position
        {
            get => _read;
            set => Seek(value, SeekOrigin.Begin);
        }
    }
}
