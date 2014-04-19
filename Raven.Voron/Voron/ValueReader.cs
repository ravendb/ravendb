using System;
using System.IO;
using System.Text;
using Voron.Impl;
using Voron.Util;
using Voron.Util.Conversion;

namespace Voron
{
	public unsafe class ValueReader
	{
		private int _pos;
		private readonly byte[] _buffer;
	    private byte[] _tmpBuf;
		private readonly int _len;
		private readonly byte* _val;

		public ValueReader(Stream stream)
		{
			long position = stream.Position;
			_len = (int) (stream.Length - stream.Position);
			_buffer = new byte[_len];

			int pos = 0;
			while (true)
			{
				int read = stream.Read(_buffer, pos, _buffer.Length - pos);
				if (read == 0)
					break;
				pos += read;
			}
			stream.Position = position;
		}

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

		public bool EndOfData
		{
			get { return _len == _pos; }
		}

		public void Reset()
		{
			_pos = 0;
		}

		public Stream AsStream()
		{
			if (_val == null)
				return new MemoryStream(_buffer, false);
			return new UnmanagedMemoryStream(_val, _len, _len, FileAccess.Read);
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

        public int ReadLittleEndianInt32()
		{
			if (_len - _pos < sizeof (int))
				throw new EndOfStreamException();
	        var buffer = EnsureTempBuffer(sizeof (int));

			Read(buffer, 0, sizeof (int));

			return BitConverter.ToInt32(buffer, 0);
		}


		public long ReadLittleEndianInt64()
		{
			if (_len - _pos < sizeof (long))
				throw new EndOfStreamException();
			var buffer = EnsureTempBuffer(sizeof(long));

			Read(buffer, 0, sizeof (long));

			return BitConverter.ToInt64(buffer, 0);
		}

		public int ReadBigEndianInt32()
		{
			if (_len - _pos < sizeof(int))
				throw new EndOfStreamException();
			var buffer = EnsureTempBuffer(sizeof(int));

			Read(buffer, 0, sizeof(int));

			return EndianBitConverter.Big.ToInt32(buffer, 0);
		}


		public long ReadbigEndianInt64()
		{
			if (_len - _pos < sizeof(long))
				throw new EndOfStreamException();
			var buffer = EnsureTempBuffer(sizeof(long));

			Read(buffer, 0, sizeof(long));

			return EndianBitConverter.Big.ToInt64(buffer, 0);
		}


	    private byte[] EnsureTempBuffer(int size)
	    {
		    if (_tmpBuf != null && _tmpBuf.Length >= size)
			    return _tmpBuf;
		    return _tmpBuf = new byte[Utils.NearestPowerOfTwo(size)];
	    }

		public string ToStringValue()
		{
	        int length = _len - _pos;
	        int used;
	        return Encoding.UTF8.GetString(ReadBytes(length, out used), 0, used);
		}

		public override string ToString()
		{
			var old = _pos;
			var stringValue = ToStringValue();
			_pos = old;
			return stringValue;
		}

	    public byte[] ReadBytes(int length, out int used)
		{
			int size = Math.Min(length, _len - _pos);
	        var buffer = EnsureTempBuffer(length);
			used = Read(buffer, 0, size);
			return buffer;
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
			int r = CompareData(other, Math.Min(Length, other.Length));
			if (r != 0)
				return r;
			return Length - other.Length;
		}

		private int CompareData(ValueReader other, int len)
		{
			if (_buffer != null)
			{
				fixed (byte* a = _buffer)
				{
					if (other._buffer != null)
					{
						fixed (byte* b = other._buffer)
						{
							return NativeMethods.memcmp(a, b, len);
						}
					}
					return NativeMethods.memcmp(a, other._val, len);
				}
			}
			if (other._buffer != null)
			{
				fixed (byte* b = other._buffer)
				{
					return NativeMethods.memcmp(_val, b, len);
				}
			}
			return NativeMethods.memcmp(_val, other._val, len);
		}

		public Slice AsSlice()
		{
			if (_len >= ushort.MaxValue)
				throw new InvalidOperationException("Cannot convert to slice, len is too big: " + _len);
			if (_buffer != null)
				return new Slice(_buffer, (ushort) _len);
			return new Slice(_val, (ushort) _len);
		}
	}
}
