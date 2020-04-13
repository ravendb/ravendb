using System;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Utils;

namespace Voron
{
    public unsafe struct ValueReader
    {
        [ThreadStatic]
        private static byte[] tmpBuf;
        static ValueReader()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => tmpBuf = null;
        }

        private int _pos;

        private readonly int _len;
        private readonly byte* _val;

        public byte* Base => _val;

        public ValueReader(byte* val, int len)
        {
            _val = val;
            _len = len;
            _pos = 0;
        }

       
        public int Length
        {
            get { return _len; }
        }

        public bool EndOfData => _len == _pos;

        public void Reset()
        {
            _pos = 0;
        }

        public Stream AsStream()
        {
            return new UnmanagedMemoryStream(_val, _len, _len, FileAccess.Read);
        }

        public int Read(byte[] buffer, int offset, int count)
        {
            fixed (byte* b = buffer)
                return Read(b + offset, count);
        }

        public void Skip(int size)
        {
            _pos += size;
        }

        public int Read(byte* buffer, int count)
        {
            count = Math.Min(count, _len - _pos);
            if (count <= 0)
                return 0;
            Memory.Copy(buffer, _val + _pos, count);
            _pos += count;

            return count;
        }

        public int ReadLittleEndianInt32()
        {
            if (_len - _pos < sizeof (int))
                throw new EndOfStreamException();
            var val = *(int*) (_val + _pos);

            _pos += sizeof (int);

            return val;
        }


        public long ReadLittleEndianInt64()
        {
            if (_len - _pos < sizeof (long))
                throw new EndOfStreamException();
            var val = *(long*) (_val + _pos);

            _pos += sizeof (long);

            return val;
        }

        public int ReadBigEndianInt32()
        {
            if (_len - _pos < sizeof (int))
                throw new EndOfStreamException();

            int val = *(int*) (_val + _pos);

            _pos += sizeof (int);

            return Bits.SwapBytes(val);
        }

        public byte ReadByte()
        {
            if (_len - _pos < sizeof(byte))
                throw new EndOfStreamException();

            byte val = *(_val + _pos);

            _pos += sizeof(byte);

            return val;
        }

        public long ReadBigEndianInt64()
        {
            if (_len - _pos < sizeof (long))
                throw new EndOfStreamException();
            
            var val = *(long*) (_val + _pos);

            return Bits.SwapBytes(val);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte[] EnsureTempBuffer(int size)
        {
            if (tmpBuf != null && tmpBuf.Length >= size)
                return tmpBuf;

            return tmpBuf = new byte[Bits.PowerOf2(size)];
        }

        public string ReadString(int length)
        {
            var arraySegment = ReadBytes(length);
            return Encodings.Utf8.GetString(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
        }

        public string ToStringValue()
        {
            int length = _len - _pos;
            var arraySegment = ReadBytes(length);
            return Encodings.Utf8.GetString(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
        }

        public override string ToString()
        {
            var old = _pos;
            var stringValue = ToStringValue();
            _pos = old;
            return stringValue;
        }

        public ArraySegment<byte> ReadBytes(int length)
        {
            int size = Math.Min(length, _len - _pos);
            var buffer = EnsureTempBuffer(length);
            var used = Read(buffer, 0, size);
            return new ArraySegment<byte>(buffer, 0, used);
        }

        public void CopyTo(Stream stream)
        {
            var buffer = new byte[4096];
            while (true)
            {
                int read = Read(buffer, 0, buffer.Length);
                if (read == 0)
                    return;

                stream.Write(buffer, 0, read);
            }
        }

        public int CompareTo(ValueReader other)
        {
            int size = Math.Min(Length, other.Length);

            int r = Memory.CompareInline(_val, other._val, size);

            if (r != 0)
                return r;

            return Length - other.Length;
        }

        public ByteStringContext.InternalScope AsSlice(ByteStringContext context, out Slice str)
        {
            if (_len >= ushort.MaxValue)
                throw new InvalidOperationException("Cannot convert to slice, len is too big: " + _len);

            return Slice.From(context, _val, _len, out str);
        }

        public Span<byte> AsSpan()
        {
            return new Span<byte>(_val, _len);
        }

        public ByteStringContext.InternalScope AsPartialSlice(ByteStringContext context, int removeFromEnd, out Slice str)
        {
            if (_len >= ushort.MaxValue)
                throw new InvalidOperationException("Cannot convert to slice, len is too big: " + _len);

            return Slice.From(context, _val, _len - removeFromEnd, out str);
        }
    }
}
