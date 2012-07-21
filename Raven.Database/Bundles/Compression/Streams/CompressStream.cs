using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Compression.Plugin;

namespace Raven.Bundles.Compression.Streams
{
	internal class CompressStream : Stream
	{
		private readonly DeflateStream deflateStream;

		public CompressStream(Stream underlyingStream)
		{
			try
			{
				var magic = BitConverter.GetBytes(DocumentCompression.CompressFileMagic);
				underlyingStream.Write(magic, 0, magic.Length);

				this.deflateStream = new DeflateStream(underlyingStream, CompressionMode.Compress, leaveOpen: false);
			}
			catch
			{
				underlyingStream.Close();
				throw;
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

		public override int Read(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException();
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			deflateStream.Write(buffer, offset, count);
		}

		public override void Flush()
		{
			deflateStream.Flush();
		}

		public override long Position
		{
			get
			{
				throw new NotSupportedException();
			}
			set
			{
				throw new NotSupportedException();
			}
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException();
		}

		public override long Length
		{
			get { throw new NotSupportedException(); }
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException();
		}

		public override void Close()
		{
			deflateStream.Close();
		}
	}
}
