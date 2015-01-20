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


	    public void WriteString(string s)
	    {
		    var stringBytes = Encoding.UTF8.GetBytes(s);
			Array.Copy(stringBytes,_buffer,stringBytes.Length);
		    _pos += (stringBytes.Length);
	    }

		public void WriteBigEndian(byte b)
		{
			_buffer[_pos] = b;
			_pos += sizeof(byte);
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