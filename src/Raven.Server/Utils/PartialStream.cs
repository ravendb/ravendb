using System;
using System.IO;

namespace Raven.Server.Utils
{
    public class PartialStream : Stream
    {
        private readonly Stream _inner;
        private int _size;

        public PartialStream(Stream inner, int size)
        {
            _inner = inner;
            _size = size;
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
            if (_size == 0)
                return 0;
            var actualCount = Math.Min(_size, count);
            var read = _inner.Read(buffer, offset, actualCount);
            _size -= read;
            return read;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override long Length
        {
            get { throw new NotSupportedException(); }
        }

        public override long Position
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }

        protected override void Dispose(bool disposing)
        {
            while (_size > 0)
            {
                if (_inner.ReadByte() == -1)
                    break;
                _size--;
            }
            base.Dispose(disposing);
        }
    }
}
