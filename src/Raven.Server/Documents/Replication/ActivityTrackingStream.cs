using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Replication
{
    /// <summary>
    /// A lightweight wrapper around a Stream that tracks the total number of bytes read.
    /// Used to distinguish between a "hanging" connection (0 bytes read over time)
    /// and a "slow but active" connection (bytes are flowing, just slowly).
    /// </summary>
    public sealed class ActivityTrackingStream : Stream
    {
        private readonly Stream _inner;
        private long _totalBytesRead;

        public ActivityTrackingStream(Stream inner)
        {
            _inner = inner;
        }

        // Volatile read to ensure the latest value is seen by the monitoring thread
        public long TotalBytesRead => Interlocked.Read(ref _totalBytesRead);

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var read = await _inner.ReadAsync(buffer, offset, count, cancellationToken);
            if (read > 0)
                Interlocked.Add(ref _totalBytesRead, read);

            return read;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var read = _inner.Read(buffer, offset, count);
            if (read > 0)
                Interlocked.Add(ref _totalBytesRead, read);

            return read;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var read = await _inner.ReadAsync(buffer, cancellationToken);
            if (read > 0)
                Interlocked.Add(ref _totalBytesRead, read);

            return read;
        }

        // Standard delegation for other members
        public override void Flush() => _inner.Flush();
        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
        public override void SetLength(long value) => _inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => _inner.Length;
        public override long Position { get => _inner.Position; set => _inner.Position = value; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _inner.Dispose();

            base.Dispose(disposing);
        }
    }
}
