using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Lucene.Net.Store;
using Microsoft.Win32.SafeHandles;
using Raven.Abstractions.Extensions;
using Sparrow;
using Voron.Platform.Win32;

namespace Raven.Server.Indexing
{
    public unsafe class VoronIndexInput : IndexInput
    {
        private readonly int _size;
        private Stream _stream;
        private readonly byte* _basePtr;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private class MmapStream : Stream
        {
            private readonly byte* ptr;
            private readonly long len;
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
                get { return false; }
            }
            public override long Length
            {
                get { return len; }
            }
            public override long Position { get { return pos; } set { pos = value; } }
        }

        public VoronIndexInput(byte* basePtr, int size)
        {
            _basePtr = basePtr;
            _size = size;
            _stream = new MmapStream(_basePtr, _size);
        }

        public override object Clone()
        {
            if (_cts.IsCancellationRequested)
                throw new ObjectDisposedException("CodecIndexInput");

            var clone = (VoronIndexInput)base.Clone();
            GC.SuppressFinalize(clone);
            clone._stream = new MmapStream(_basePtr, _size)
            {
                Position = _stream.Position
            };
            return clone;
        }

        public override byte ReadByte()
        {
            if (_cts.IsCancellationRequested)
                throw new ObjectDisposedException("CodecIndexInput");
            var readByte = _stream.ReadByte();
            if (readByte == -1)
                throw new EndOfStreamException();
            return (byte)readByte;
        }

        public override void ReadBytes(byte[] b, int offset, int len)
        {
            if (_cts.IsCancellationRequested)
                throw new ObjectDisposedException("CodecIndexInput");
            _stream.ReadEntireBlock(b, offset, len);
        }

        protected override void Dispose(bool disposing)
        {

        }

        public override void Seek(long pos)
        {
            if (_cts.IsCancellationRequested)
                throw new ObjectDisposedException("CodecIndexInput");
            _stream.Seek(pos, SeekOrigin.Begin);
        }

        public override long Length()
        {
            return _stream.Length;
        }

        public override long FilePointer
        {
            get
            {
                if (_cts.IsCancellationRequested)
                    throw new ObjectDisposedException("CodecIndexInput");
                return _stream.Position;
            }
        }
    }
}