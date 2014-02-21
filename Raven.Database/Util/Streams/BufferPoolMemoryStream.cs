// -----------------------------------------------------------------------
//  <copyright file="BufferPoolMemoryStream.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;

namespace Raven.Database.Util.Streams
{
    public class BufferPoolMemoryStream : Stream
    {
        private readonly IBufferPool _bufferPool;
        protected byte[] _buffer;
        protected long _length;
        private int _position;

        public BufferPoolMemoryStream(IBufferPool bufferPool)
        {
            _bufferPool = bufferPool;
            _buffer = _bufferPool.TakeBuffer(8 * 1024);
        }

        protected override void Dispose(bool disposing)
        {
            _bufferPool.ReturnBuffer(_buffer);
            base.Dispose(disposing);
        }

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Position = offset;
                    break;
                case SeekOrigin.Current:
                    Position += offset;
                    break;
                case SeekOrigin.End:
                    Position = Length - offset;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("origin");
            }

            return Position;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var read = (int)Math.Min(count, _length - _position);
            Buffer.BlockCopy(_buffer, _position, buffer, offset, read);
            _position += read;
            return read;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            EnsureCapacity(_position + count);
            Buffer.BlockCopy(buffer, offset, _buffer, _position, count);
            _position += count;
            _length = Math.Max(_length, _position);
        }

        public override void SetLength(long value)
        {
            EnsureCapacity(value);
            _length = value;
        }

        private void EnsureCapacity(long value)
        {
            if (value <= _buffer.Length)
                return;
            var newBuffer = _bufferPool.TakeBuffer(_buffer.Length * 2);
            Buffer.BlockCopy(_buffer, 0, newBuffer, 0, _position);
            _bufferPool.ReturnBuffer(_buffer);
            _buffer = newBuffer;
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override long Length { get { return _length; } }

        public override long Position
        {
            get { return _position; }
            set
            {
                if (value < 0 || value >= _length)
                    throw new ArgumentOutOfRangeException("value", "Cannot set position to lower than 0 or higher than the length");
                _position = (int)value;
            }
        }
    }
}