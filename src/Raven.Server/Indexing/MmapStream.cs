using System;
using System.IO;

using Sparrow;

namespace Raven.Server.Indexing
{
    unsafe class MmapStream : Stream
    {
        private byte* _ptr;
        private long _len;
        private long _pos;

        public MmapStream(byte* ptr, long len)
        {
            _ptr = ptr;
            _len = len;
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
                    Position = _len + offset;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("origin", origin, null);
            }
            return Position;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int ReadByte()
        {
            if (Position == _len)
                return -1;
            return _ptr[_pos++];
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_pos == _len)
                return 0;
            if (count > _len - _pos)
            {
                count = (int)(_len - _pos);
            }
            fixed (byte* dst = buffer)
            {
                Memory.Copy(dst + offset, _ptr + _pos, count);
            }
            _pos += count;
            return count;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override long Length => _len;
        public override long Position { get { return _pos; } set { _pos = value; } }

        public void Set(byte* buffer, int size)
        {
            _ptr = buffer;
            _len = size;
            _pos = 0;
        }
    }
}