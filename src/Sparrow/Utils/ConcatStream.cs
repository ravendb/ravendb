using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Utils
{
    internal class ConcatStream : Stream
    {
        private readonly RentedBuffer _prefix;
        private readonly Stream _remaining;
        private bool _disposed;

        public class RentedBuffer
        {
            public byte[] Buffer;
            public int Offset;
            public int Count;
        }

        public ConcatStream(RentedBuffer prefix, Stream remaining)
        {
            _prefix = prefix;
            _remaining = remaining;
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
            if (_disposed)
                ThrowDisposedException();

            if (_prefix.Count <= 0)
                return _remaining.Read(buffer, offset, count);

            int read = ReadFromBuffer(buffer, offset, count);

            return read;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (_disposed)
                ThrowDisposedException();

            if (_prefix.Count <= 0)
                return _remaining.ReadAsync(buffer, offset, count, cancellationToken);

            int read = ReadFromBuffer(buffer, offset, count);

            return Task.FromResult(read);
        }

        private int ReadFromBuffer(byte[] buffer, int offset, int count)
        {
            var read = Math.Min(_prefix.Count, count);
            Buffer.BlockCopy(_prefix.Buffer, _prefix.Offset, buffer, offset, read);
            _prefix.Count -= read;
            _prefix.Offset += read;
            if (_prefix.Count == 0)
            {
                ArrayPool<byte>.Shared.Return(_prefix.Buffer);
                _prefix.Buffer = null;
            }

            return read;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        private void ThrowDisposedException()
        {
            throw new ObjectDisposedException($"{nameof(_remaining)} stream was already disposed.");
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }

        protected override void Dispose(bool disposing)
        {
            _disposed = true;

            if (_prefix.Buffer != null)
            {
                ArrayPool<byte>.Shared.Return(_prefix.Buffer);
                _prefix.Buffer = null;
            }

            _remaining?.Dispose();

            base.Dispose(disposing);
        }
    }
}
