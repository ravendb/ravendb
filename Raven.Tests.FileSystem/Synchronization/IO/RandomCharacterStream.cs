using System;
using System.IO;

namespace RavenFS.Tests.Synchronization.IO
{
	public class RandomCharacterStream : Stream
	{
		private const string _chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuwvxyz";
		private readonly long _length;
		private readonly Random _random;
		private long _position;

		public RandomCharacterStream(long length, int? seed = null)
		{
			_length = length;
			_random = seed == null ? new Random() : new Random(seed.Value);
		}

		public override bool CanRead
		{
			get { return true; }
		}

		public override bool CanSeek
		{
			get { return false; }
		}

		public override bool CanWrite
		{
			get { return false; }
		}

		public override long Length
		{
			get { return _length; }
		}

		public override long Position
		{
			get { return _position; }
			set { throw new NotSupportedException(); }
		}

		public override void Flush()
		{
			throw new NotSupportedException();
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException();
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			var length = Math.Min(_length - _position, count);
			if (length < 1)
				return 0;
			
			var newValues = new byte[length];
			for (var i = 0; i < length; i++)
			{
				newValues[i] = (byte) _chars[_random.Next(_chars.Length)];
			}

			Array.Copy(newValues, 0, buffer, offset, length);
			_position += length;
			return Convert.ToInt32(length);
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException();
		}
	}
}