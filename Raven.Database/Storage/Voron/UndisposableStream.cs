// -----------------------------------------------------------------------
//  <copyright file="us.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

namespace Raven.Database.Storage.Voron
{
	public class UndisposableStream : Stream
	{
		private readonly Stream inner;

		public UndisposableStream(Stream inner)
		{
			this.inner = inner;
		}

		public override void Flush()
		{
			inner.Flush();
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			return inner.Seek(offset, origin);
		}

		public override void SetLength(long value)
		{
			inner.SetLength(value);
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			return inner.Read(buffer, offset, count);
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			inner.Write(buffer, offset, count);
		}

		public override bool CanRead
		{
			get { return inner.CanRead; }
		}
		public override bool CanSeek
		{
			get { return inner.CanSeek; }
		}
		public override bool CanWrite
		{
			get { return inner.CanWrite; }
		}
		public override long Length
		{
			get { return inner.Length; }
		}
		public override long Position
		{
			get { return inner.Position; }
			set { inner.Position = value; }
		}
	}

}