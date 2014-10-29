using System;
using System.IO;

namespace Raven.Database.Server.RavenFS.Util
{
	public class NarrowedStream : Stream
	{
		public Stream Source { get; private set; }
		public long From { get; private set; }
		public long To { get; private set; }

		public NarrowedStream(Stream source, long from, long to)
		{
			if (!source.CanRead)
			{
				throw new ArgumentException("Source must be readable", "source");
			}
			if (!source.CanSeek)
			{
				throw new ArgumentException("Source must be seekable", "source");
			}
			if (from > source.Length)
			{
				throw new ArgumentOutOfRangeException("from");
			}
			if (to > source.Length)
			{
				throw new ArgumentOutOfRangeException("to");
			}

			Source = source;
			From = from;
			To = to;
			Seek(0, SeekOrigin.Begin);
		}

		public override void Flush()
		{
			Source.Flush();
		}

		public override sealed long Seek(long offset, SeekOrigin origin)
		{
			switch (origin)
			{
				case SeekOrigin.Begin:
					offset = From + offset;
					break;
				case SeekOrigin.Current:
					break;
				case SeekOrigin.End:
					offset = To - offset;
					break;
				default:
					throw new ArgumentOutOfRangeException("origin", origin, "Unknown SeekOrigin");
			}
			if (offset < From)
			{
				throw new ArgumentOutOfRangeException("offset",
													  "An attempt was made to move the file pointer before the beginning of the file.");
			}
			Source.Seek(offset, SeekOrigin.Begin);
			return Position;
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			if (Position >= Length)
				return 0;

			var startingPosition = Position;
			var read = Source.Read(buffer, offset, count);
			var preResult = Math.Min(Length - startingPosition, read);
			return Convert.ToInt32(preResult);
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException();
		}

		public override bool CanRead
		{
			get { return true; }
		}

		public override bool CanSeek
		{
			get { return true; }
		}

		public override bool CanWrite
		{
			get { return false; }
		}

		public override long Length
		{
			get { return To - From + 1; }
		}

		public override long Position
		{
			get { return Source.Position - From; }
			set
			{
				if (value < 0)
					throw new ArgumentOutOfRangeException("value", "Non-negative number required");

				Seek(value, SeekOrigin.Begin);
			}
		}
	}
}
