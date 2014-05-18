using System;
using System.IO;

namespace Raven.Database.Server.RavenFS.Util
{
	public class CombinedStream : Stream
	{
		private readonly Stream[] underlyingStreams;
		private readonly long[] underlyingStartingPositions;
		private long position;
		private readonly long totalLength;
		private int index;

		public CombinedStream(params Stream[] underlyingStreams)
		{
			if (underlyingStreams == null)
				throw new ArgumentNullException("underlyingStreams");
			foreach (var stream in underlyingStreams)
			{
				if (stream == null)
					throw new ArgumentNullException("underlyingStreams");
				if (!stream.CanRead)
					throw new InvalidOperationException("CanRead not true for all streams");
				if (!stream.CanSeek)
					throw new InvalidOperationException("CanSeek not true for all streams");
			}

			this.underlyingStreams = new Stream[underlyingStreams.Length];
			underlyingStartingPositions = new long[underlyingStreams.Length];
			Array.Copy(underlyingStreams, this.underlyingStreams, underlyingStreams.Length);

			position = 0;
			index = 0;

			underlyingStartingPositions[0] = 0;
			for (var i = 1; i < underlyingStartingPositions.Length; i++)
			{
				underlyingStartingPositions[i] =
					underlyingStartingPositions[i - 1] +
					this.underlyingStreams[i - 1].Length;
			}

			totalLength =
				underlyingStartingPositions[underlyingStartingPositions.Length - 1] +
				this.underlyingStreams[this.underlyingStreams.Length - 1].Length;
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
			foreach (var stream in underlyingStreams)
			{
				stream.Flush();
			}
		}

		public override long Length
		{
			get
			{
				return totalLength;
			}
		}

		public override long Position
		{
			get
			{
				return position;
			}

			set
			{
				if (value < 0 || value > totalLength)
					throw new ArgumentOutOfRangeException("Position");

				position = value;
				if (value == totalLength)
				{
					index = underlyingStreams.Length - 1;
					position = underlyingStreams[index].Length;
				}

				else
				{
					while (index > 0 && position < underlyingStartingPositions[index])
					{
						index--;
					}

					while (index < underlyingStreams.Length - 1 &&
						   position >= underlyingStartingPositions[index] + underlyingStreams[index].Length)
					{
						index++;
					}
				}
			}
		}

		public override int Read(Byte[] buffer, int offset, int count)
		{
			var result = 0;
			while (count > 0)
			{
				underlyingStreams[index].Position = position - underlyingStartingPositions[index];
				var bytesRead = underlyingStreams[index].Read(buffer, offset, count);
				result += bytesRead;
				offset += bytesRead;
				count -= bytesRead;
				position += bytesRead;

				if (count > 0)
				{
					if (index < underlyingStreams.Length - 1)
						index++;
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