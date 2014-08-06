// -----------------------------------------------------------------------
//  <copyright file="CachingStream.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

namespace Raven.Abstractions.Connection
{
	public class CachingStream : Stream
	{
		private readonly Stream inner;

		private readonly int sizeOfCache;

		private int spaceLeft;

		public byte[] Cache { get; private set; }

		public CachingStream(Stream stream, int sizeOfCache)
		{
			inner = stream;
			this.sizeOfCache = sizeOfCache;
			spaceLeft = sizeOfCache;
			Cache = new byte[sizeOfCache];
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
			var read = inner.Read(buffer, offset, count);
			if (read <= 0)
				return read;

			var sizeToCopy = Math.Min(read, sizeOfCache);
			var spaceRequired = spaceLeft - sizeToCopy;
			if (spaceRequired < 0)
			{
				var startIndex = Math.Abs(spaceRequired);
				var length = sizeOfCache - spaceLeft - startIndex;
				var tempCache = new byte[sizeOfCache];
				Array.Copy(Cache, startIndex, tempCache, 0, length);
				spaceLeft = sizeOfCache - length;
				Cache = tempCache;
			}

			Array.Copy(buffer, 0, Cache, sizeOfCache - spaceLeft, sizeToCopy);
			spaceLeft -= sizeToCopy;

			return read;
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			inner.Write(buffer, offset, count);
		}

		public override bool CanRead
		{
			get
			{
				return inner.CanRead;
			}
		}

		public override bool CanSeek
		{
			get
			{
				return inner.CanSeek;
			}
		}

		public override bool CanWrite
		{
			get
			{
				return inner.CanWrite;
			}
		}

		public override long Length
		{
			get
			{
				return inner.Length;
			}
		}

		public override long Position
		{
			get
			{
				return inner.Position;
			}

			set
			{
				inner.Position = value;
			}
		}
	}
}