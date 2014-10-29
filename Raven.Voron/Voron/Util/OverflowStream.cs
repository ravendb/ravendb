namespace Voron.Util
{
	using System;
	using System.Collections.Generic;
	using System.IO;

	public class OverflowStream : Stream
	{
		private readonly Stream[] _underlyingStreams;

		private readonly long[] _underlyingStartingPositions;

		private long _position;

		private readonly long _totalLength;

		private int _index;

		public OverflowStream(IList<Stream> underlyingStreams)
		{
			if (underlyingStreams == null) throw new ArgumentNullException("underlyingStreams");

			_underlyingStreams = new Stream[underlyingStreams.Count];
			_underlyingStartingPositions = new long[underlyingStreams.Count];

			for (var i = 0; i < underlyingStreams.Count; i++)
			{
				var stream = underlyingStreams[i];

				if (stream == null) throw new ArgumentNullException("underlyingStreams");
				if (!stream.CanRead) throw new InvalidOperationException("CanRead not true for all streams");
				if (!stream.CanSeek) throw new InvalidOperationException("CanSeek not true for all streams");

				_underlyingStreams[i] = stream;
			}

			_position = 0;
			_index = 0;

			_underlyingStartingPositions[0] = 0;
			for (var i = 1; i < _underlyingStartingPositions.Length; i++)
				_underlyingStartingPositions[i] = _underlyingStartingPositions[i - 1] + _underlyingStreams[i - 1].Length;

			_totalLength = _underlyingStartingPositions[_underlyingStartingPositions.Length - 1] + _underlyingStreams[_underlyingStreams.Length - 1].Length;
		}

		public override Boolean CanRead
		{
			get
			{
				return true;
			}
		}

		public override Boolean CanSeek
		{
			get
			{
				return true;
			}
		}

		public override Boolean CanWrite
		{
			get
			{
				return false;
			}
		}

		public override void Flush()
		{
			foreach (var stream in _underlyingStreams)
			{
				stream.Flush();
			}
		}

		public override long Length
		{
			get
			{
				return _totalLength;
			}
		}

		public override long Position
		{
			get
			{
				return _position;
			}

			set
			{
				if (value < 0 || value > _totalLength) throw new ArgumentOutOfRangeException("Position");

				_position = value;
				if (value == _totalLength)
				{
					_index = _underlyingStreams.Length - 1;
					_position = _underlyingStreams[_index].Length;
				}

				else
				{
					while (_index > 0 && _position < _underlyingStartingPositions[_index])
					{
						_index--;
					}

					while (_index < _underlyingStreams.Length - 1 && _position >= _underlyingStartingPositions[_index] + _underlyingStreams[_index].Length)
					{
						_index++;
					}
				}
			}
		}

		public override int Read(Byte[] buffer, int offset, int count)
		{
			var result = 0;
			while (count > 0)
			{
				_underlyingStreams[_index].Position = _position - _underlyingStartingPositions[_index];
				var bytesRead = _underlyingStreams[_index].Read(buffer, offset, count);
				result += bytesRead;
				offset += bytesRead;
				count -= bytesRead;
				_position += bytesRead;

				if (count > 0)
				{
					if (_index < _underlyingStreams.Length - 1) 
						_index++;
					else 
						break;
				}
			}

			return result;
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			switch (origin)
			{
				case SeekOrigin.Begin:
					Position = offset;
					break;

				case SeekOrigin.Current:
					Position += offset;
					break;

				case SeekOrigin.End:
					Position = Length + offset;
					break;
			}

			return Position;
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException("The method or operation is not supported by CombinedStream.");
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException("The method or operation is not supported by CombinedStream.");
		}
	}
}