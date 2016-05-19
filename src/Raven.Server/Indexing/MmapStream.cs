using System;
using System.IO;

using Sparrow;

namespace Raven.Server.Indexing
{
    unsafe class MmapStream : Stream
    {
        private byte* ptr;
        private long len;
        private long pos;

        public MmapStream(byte* ptr, long len)
        {
            this.ptr = ptr;
            this.len = len;
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
                    Position = len + offset;
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
            if (Position == len)
                return -1;
            return ptr[pos++];
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (pos == len)
                return 0;
            if (count > len - pos)
            {
                count = (int)(len - pos);
            }
            fixed (byte* dst = buffer)
            {
                Memory.CopyInline(dst + offset, ptr + pos, count);
            }
            pos += count;
            return count;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override long Length => len;
        public override long Position { get { return pos; } set { pos = value; } }

        public void Set(byte* buffer, int size)
        {
            this.ptr = buffer;
            this.len = size;
            pos = 0;
        }
    }
}