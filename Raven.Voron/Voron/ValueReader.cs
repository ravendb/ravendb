using System;
using System.IO;
using System.Text;
using Voron.Impl;

namespace Voron
{
    public unsafe class ValueReader
    {
        private readonly byte* _val;
        private readonly byte[] _buffer;
        private readonly int _len;
        private int _pos;
        public int Length { get { return _len; } }

        public void Reset()
        {
            _pos = 0;
        }

        public ValueReader(Stream stream)
        {
            var position = stream.Position;
            _len = (int)(stream.Length - stream.Position);
            _buffer = new byte[_len];

            int pos = 0;
            while (true)
            {
                var read = stream.Read(_buffer, pos, _buffer.Length - pos);
                if (read == 0)
                    break;
                pos += read;
            }
            stream.Position = position;

        }

	    public Stream AsStream()
	    {
		    return new UnmanagedMemoryStream(_val, _len, _len, FileAccess.Read);
	    }

        public ValueReader(byte* val, int len)
        {
            _val = val;
            _len = len;
        }

        public int Read(byte[] buffer, int offset, int count)
        {
            fixed (byte* b = buffer)
                return Read(b + offset, count);
        }

        public int Read(byte* buffer, int count)
        {
            count = Math.Min(count, _len - _pos);

            if (_val == null)
            {
                fixed (byte* b = _buffer)
                    NativeMethods.memcpy(buffer, b + _pos, count);
            }
            else
            {
                NativeMethods.memcpy(buffer, _val + _pos, count);
            }
            _pos += count;

            return count;
        }

        public int ReadInt32()
        {
            if (_len - _pos < sizeof(int))
                throw new EndOfStreamException();
            var buffer = new byte[sizeof(int)];

            Read(buffer, 0, sizeof (int));

            return BitConverter.ToInt32(buffer, 0);
        }

        public long ReadInt64()
        {
            if (_len - _pos < sizeof(long))
                throw new EndOfStreamException();
            var buffer = new byte[sizeof(long)];

            Read(buffer, 0, sizeof(long));

            return BitConverter.ToInt64(buffer, 0);
        }

        public string ToStringValue()
        {
            return Encoding.UTF8.GetString(ReadBytes(_len - _pos));
        }

        public byte[] ReadBytes(int length)
        {
            var size = Math.Min(length, _len - _pos);
            var buffer = new byte[size];
            Read(buffer, 0, size);
            return buffer;
        }

        public void CopyTo(Stream stream)
        {
            var buffer = new byte[4096];
            while (true)
            {
                var read = Read(buffer, 0, buffer.Length);
                if (read == 0)
                    return;
                stream.Write(buffer, 0, read);
            }
        }

    }
}