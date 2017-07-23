using System;
using System.IO;

namespace Raven.Server.Documents.PeriodicBackup.Azure
{
    class SubStream : Stream
    {
        private Stream _baseStream;
        private readonly long _length;
        private long _position;

        public SubStream(Stream baseStream, long offset, long length)
        {
            if (baseStream == null)
                throw new ArgumentNullException(nameof(baseStream));
            if (baseStream.CanRead == false)
                throw new ArgumentException("can't read base stream");
            if (baseStream.CanSeek == false)
                throw new ArgumentException("can't seek in base stream");
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset");
            if (length < 0)
                throw new ArgumentOutOfRangeException("length");

            _baseStream = baseStream;
            _length = length;

            baseStream.Seek(offset, SeekOrigin.Current);
        }
        public override int Read(byte[] buffer, int offset, int count)
        {
            CheckDisposed();

            var remaining = _length - _position;
            if (remaining <= 0)
                return 0;

            if (remaining < count)
                count = (int)remaining;

            var read = _baseStream.Read(buffer, offset, count);
            _position += read;
            return read;
        }

        private void CheckDisposed()
        {
            if (_baseStream == null)
                throw new ObjectDisposedException(GetType().Name);
        }

        public override long Length
        {
            get
            {
                CheckDisposed();
                return _length;
            }
        }

        public override bool CanRead
        {
            get
            {
                CheckDisposed();
                return true;
            }
        }

        public override bool CanWrite
        {
            get
            {
                CheckDisposed();
                return false;
            }
        }

        public override bool CanSeek
        {
            get
            {
                CheckDisposed();
                return false;
            }
        }

        public override long Position
        {
            get
            {
                CheckDisposed();
                return _position;
            }
            set => throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Flush()
        {
            CheckDisposed();
            _baseStream.Flush();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing == false)
                return;

            // the caller is reponsible for disposing the base stream
            _baseStream = null;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
    }
}