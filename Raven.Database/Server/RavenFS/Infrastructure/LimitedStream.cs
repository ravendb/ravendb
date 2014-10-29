using System;
using System.IO;

namespace Raven.Database.Server.RavenFS.Infrastructure
{
	public class LimitedStream : Stream
	{
		private readonly Stream inner;
		private readonly long end;

		public LimitedStream(Stream inner, long start, long end)
		{
			this.inner = inner;
			this.end = end;
			this.inner.Position = start;
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
			var actualCount = inner.Position + count > end ? end - inner.Position : count;
			return inner.Read(buffer, offset, (int)actualCount);
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
			get { return false; }
		}

		public override bool CanWrite
		{
			get { return false; }
		}

		public override long Length
		{
			get { throw new NotSupportedException(); }
		}

		public override long Position
		{
			get { throw new NotSupportedException(); }
			set { throw new NotSupportedException(); }
		}

		protected override void Dispose(bool disposing)
		{
			inner.Dispose();
			base.Dispose(disposing);
		}
	}
}