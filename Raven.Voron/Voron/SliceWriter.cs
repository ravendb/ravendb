using System;
using System.Text;
using Voron.Util.Conversion;

namespace Voron
{
    public struct SliceWriter
    {
		private int _pos;
		private readonly byte[] _buffer;
	    private static readonly EndianBitConverter BitConverter = EndianBitConverter.Big;

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

	    public void Write(string s)
	    {
			var stringBytes = Encoding.UTF8.GetBytes(s, 0, s.Length, _buffer, _pos);
		    _pos += stringBytes;
	    }

		public void WriteBytes(byte[] bytes)
	    {
			Array.Copy(bytes, 0, _buffer, _pos, bytes.Length);
			_pos += (bytes.Length);
		}

		public void Write(byte b)
		{
			_buffer[_pos] = b;
			_pos += sizeof(byte);
		}

		public void Write(char i)
		{
			BitConverter.CopyBytes(i, _buffer, _pos);
			_pos += sizeof(char);
		}

		public void Write(double d)
		{
			BitConverter.CopyBytes(d, _buffer, _pos);
			_pos += sizeof(double);
		}

		public void Write(int i)
        {
			BitConverter.CopyBytes(i, _buffer, _pos);
            _pos += sizeof(int);
		}

		public void Write(long l)
        {
			BitConverter.CopyBytes(l, _buffer, _pos);
            _pos += sizeof(long);
		}

        public void Write(short s)
        {
			BitConverter.CopyBytes(s, _buffer, _pos);
            _pos += sizeof(short);
		}

        public Slice CreateSlice()
        {
			return new Slice(_buffer, (ushort)(_pos));
        }

	    public void Reset()
	    {
		    _pos = 0;
	    }

		public Slice CreateSlice(int pos)
		{
			return new Slice(_buffer, (ushort)pos);
		}
    }
}