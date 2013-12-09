using System;
using System.IO;

namespace RavenFS.Tests.Synchronization.IO
{
	public class RandomlyModifiedStream : Stream
	{
		private readonly double _probability;
		private readonly Random _random;
		private readonly Stream _source;


		public RandomlyModifiedStream(Stream source, double probability, int? seed = null)
		{
			_source = source;
			_probability = probability;

			if (seed != null)
			{
				_random = new Random(seed.Value);
			}
			else
			{
				_random = new Random();
			}
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
			get { return _source.Length; }
		}

		public override long Position
		{
			get { return _source.Position; }
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
			var result = _source.Read(buffer, offset, count);
			if (_random.NextDouble() < _probability)
			{
				var oneByte = new byte[1];
				_random.NextBytes(oneByte);
				buffer[_random.Next(buffer.Length)] = BitConverter.GetBytes(buffer[_random.Next(buffer.Length)] ^ oneByte[0])[0];
			}
			return result;
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException();
		}
	}
}