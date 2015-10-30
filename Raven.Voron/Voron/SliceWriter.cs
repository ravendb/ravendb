using System;
using System.Text;
using Voron.Util.Conversion;

namespace Voron
{
    public struct SliceWriter
    {
        private int _pos;
        private readonly byte[] _buffer;

        public SliceWriter(int size)
        {
            _pos = 0;
            _buffer = new byte[size];
        }

        public void WriteBigEndian(int i)
        {
            EndianBitConverter.Big.CopyBytes(i, _buffer, _pos);
            _pos += sizeof (int);
        }

        public void WriteBigEndian(long l)
        {
            EndianBitConverter.Big.CopyBytes(l, _buffer, _pos);
            _pos += sizeof(long);
        }

        public void WriteBigEndian(ulong l)
        {
            EndianBitConverter.Big.CopyBytes(l, _buffer, _pos);
            _pos += sizeof(ulong);
        }

        public void WriteBigEndian(short s)
        {
            EndianBitConverter.Big.CopyBytes(s, _buffer, _pos);
            _pos += sizeof(short);
        }

        public Slice CreateSlice()
        {
            return new Slice(_buffer);
        }

        public void Write(byte[] bytes)
        {
            Write(bytes, 0, bytes.Length);
        }

        private void Write(byte[] bytes, int offset, int count)
        {
            Buffer.BlockCopy(bytes, offset, _buffer, _pos, count);
            _pos += count;
        }
    }
}
