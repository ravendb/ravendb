using Sparrow;
using System;
using System.Text;
using Voron.Util.Conversion;

namespace Voron
{
    public struct SliceWriter
    {
        private volatile int _pos;
        private readonly byte[] _buffer;

        public SliceWriter(byte[] outsideBuffer)
        {
            _buffer = outsideBuffer;
            _pos = 0;
        }

        public SliceWriter(int size)
        {
            _pos = 0;
            _buffer = new byte[size];
        }

        public void WriteBigEndian(int i)
        {
            EndianBitConverter.Big.CopyBytes(i, _buffer, _pos);
            _pos += sizeof(int);
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

        public ByteStringContext.Scope CreateSlice(ByteStringContext context, out Slice str)
        {
            return CreateSlice(context, ByteStringType.Immutable, out str);
        }

        public ByteStringContext.Scope CreateSlice(ByteStringContext context, ByteStringType type, out Slice str)
        {
            ByteString byteString;
            var scope = context.From(_buffer, 0, _buffer.Length, type, out byteString);
            str = new Slice(byteString);
            return scope;
        }

        public void Write(bool b)
        {
            _buffer[_pos] = b ? (byte)1 : (byte)0;
            _pos += sizeof(byte);
        }

        public void Write(byte b)
        {
            _buffer[_pos] = b;
            _pos += sizeof(byte);
        }

        public void Write(byte[] bytes, int? length = null)
        {
            Write(bytes, 0, length ?? bytes.Length);
        }

        private void Write(byte[] bytes, int offset, int count)
        {
            Buffer.BlockCopy(bytes, offset, _buffer, _pos, count);
            _pos += count;
        }

        public void Write(char i)
        {
            EndianBitConverter.Big.CopyBytes(i, _buffer, _pos);
            _pos += sizeof(char);
        }

        public void Write(string s)
        {
            var stringBytes = Encoding.UTF8.GetBytes(s, 0, s.Length, _buffer, _pos);
            _pos += stringBytes;
        }

        public void Reset()
        {
            _pos = 0;
        }

        public ByteStringContext.Scope CreateSlice(ByteStringContext context, int size, out Slice str)
        {
            return CreateSlice(context, size, ByteStringType.Immutable, out str);
        }

        public ByteStringContext.Scope CreateSlice(ByteStringContext context, int size, ByteStringType type, out Slice str)
        {
            ByteString byteString;
            var scope = context.From(_buffer, size, type, out byteString);
            str = new Slice(byteString);
            return scope;
        }
    }
}
