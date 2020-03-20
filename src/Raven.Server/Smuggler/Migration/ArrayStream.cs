using System;
using System.IO;
using System.Text;

namespace Raven.Server.Smuggler.Migration
{
    public class ArrayStream : Stream
    {
        private Stream _baseStream;
        private long _position;

        private readonly MemoryStream _beginningStream;

        private readonly MemoryStream _endingStream =
            new MemoryStream(Encoding.UTF8.GetBytes("}"));

        public ArrayStream(Stream baseStream, string propertyName)
        {
            if (baseStream == null)
                throw new ArgumentNullException(nameof(baseStream));

            if (baseStream.CanRead == false)
                throw new ArgumentException("Cannot read base stream");

            _beginningStream = new MemoryStream(Encoding.UTF8.GetBytes($"{{ \"{propertyName}\" : "));
            _baseStream = baseStream;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            CheckDisposed();

            int read;
            if (_beginningStream.Position < _beginningStream.Length)
            {
                read = _beginningStream.Read(buffer, offset, count);
            }
            else
            {
                read = _baseStream.Read(buffer, offset, count);
                if (read == 0)
                {
                    read = _endingStream.Read(buffer, offset, count);
                }
            }

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
                throw new NotSupportedException();
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

            // the caller is responsible for disposing the base stream
            _baseStream = null;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
    }
}
