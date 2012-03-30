using System;
using System.IO;
using System.Linq;

namespace Raven.VisualHost
{
	public class MultiStreamWriter : Stream
	{
		private Stream[] streams;

		public MultiStreamWriter(params Stream[] streams)
		{
			this.streams = streams;
		}

		public override void Flush()
		{
			foreach (var stream in streams)
			{
				stream.Flush();
			}
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotImplementedException();
		}

		public override void SetLength(long value)
		{
			throw new NotImplementedException();
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			throw new NotImplementedException();
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			foreach (var stream in streams)
			{
				stream.Write(buffer, offset, count);
			}
		}

		public override bool CanRead
		{
			get { return false; }
		}

		public override bool CanSeek
		{
			get { return false; }
		}

		public override bool CanWrite
		{
			get { return true; }
		}

		public override long Length
		{
			get { throw new NotImplementedException(); }
		}

		public override long Position
		{
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		protected override void Dispose(bool disposing)
		{
			foreach (var stream in streams.Skip(1))
			{
				stream.Dispose();
			}
		}
	}
}