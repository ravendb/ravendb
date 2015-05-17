using System;
using System.Text;
using Voron.Util.Conversion;

namespace Voron
{
    public struct SliceWriter
    {
		private int _pos;
		private readonly byte[] _buffer;

	    public SliceWriter(byte[] outsideBuffer)
	    {
		    _buffer = outsideBuffer;
			_pos = 0;
	    }

	    public SliceWriter(int size)
	    {
		    _buffer = new byte[size];
		    _pos = 0;
	    }


	    public SliceWriter WriteString(string s)
	    {
		    var stringBytes = Encoding.UTF8.GetBytes(s, 0, s.Length, _buffer, _pos);
		    _pos += stringBytes;
			return this;
	    }

		public SliceWriter WriteBytes(byte[] bytes)
	    {
			Array.Copy(bytes, _buffer, bytes.Length);
			_pos += (bytes.Length);
			return this;
		}

		public SliceWriter WriteBigEndian(byte b)
		{
			_buffer[_pos] = b;
			_pos += sizeof(byte);
			return this;
		}

		public SliceWriter WriteBigEndian(char i)
		{
			EndianBitConverter.Big.CopyBytes(i, _buffer, _pos);
			_pos += sizeof(char);
			return this;
		}

		public SliceWriter WriteBigEndian(double d)
		{
			EndianBitConverter.Big.CopyBytes(d, _buffer, _pos);
			_pos += sizeof(double);
			return this;
		}

        public SliceWriter WriteBigEndian(int i)
        {
            EndianBitConverter.Big.CopyBytes(i, _buffer, _pos);
            _pos += sizeof (int);
			return this;
		}

        public SliceWriter WriteBigEndian(long l)
        {
            EndianBitConverter.Big.CopyBytes(l, _buffer, _pos);
            _pos += sizeof(long);
			return this;
		}

        public SliceWriter WriteBigEndian(short s)
        {
            EndianBitConverter.Big.CopyBytes(s, _buffer, _pos);
            _pos += sizeof(short);
			return this;
		}

        public Slice CreateSlice()
        {
			return new Slice(_buffer, (ushort)_pos);
        }
    }
}