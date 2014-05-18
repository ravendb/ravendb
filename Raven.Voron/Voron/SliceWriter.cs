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

        public void WriteBigEndian(short s)
        {
            EndianBitConverter.Big.CopyBytes(s, _buffer, _pos);
            _pos += sizeof(short);
        }

        public Slice CreateSlice()
        {
            return new Slice(_buffer);
        }
    }
}