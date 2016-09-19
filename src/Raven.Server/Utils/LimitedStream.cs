using System;
using System.Diagnostics;
using System.IO;

namespace Raven.Server.Utils
{
    public class LimitedStream : Stream
    {
        private readonly Stream _inner;
        private readonly long _endExclusive;

        public LimitedStream(Stream inner, long start, long endExclusive)
        {
            Debug.Assert(endExclusive >= start);
            _inner = inner;
            _endExclusive = endExclusive;
            _inner.Position = start;
            Length = endExclusive - start;
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
            var actualCount = _inner.Position + count > _endExclusive ? _endExclusive - _inner.Position : count;
            return _inner.Read(buffer, offset, (int)actualCount);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length { get; }

        public override long Position
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }

        protected override void Dispose(bool disposing)
        {
            _inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
