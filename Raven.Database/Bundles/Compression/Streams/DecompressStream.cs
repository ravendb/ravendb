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
	internal class DecompressStream : Stream
	{
		private readonly StreamReaderWithUnread underlyingStream;
		private readonly bool compressed;
		private readonly DeflateStream deflateStream;
		private long pos;

		public DecompressStream(Stream underlyingStream)
		{
			this.underlyingStream = new StreamReaderWithUnread(underlyingStream);
			compressed = CheckMagicNumber();

			pos = 4;
			if (compressed)
			{
				deflateStream = new DeflateStream(underlyingStream, CompressionMode.Decompress, leaveOpen: true);
			}
		}

		private bool CheckMagicNumber()
		{
			// Read an int
			byte[] buffer = new byte[4];
			var totalCount = 0;
			while (totalCount < buffer.Length)
			{
				var count = underlyingStream.Read(buffer, 0, buffer.Length);
				if (count == 0)
				{
					// End of stream before the end of the magic number.
					underlyingStream.Unread(buffer.Take(totalCount));
					return false;
				}

				totalCount += count;
			}

			var magic = BitConverter.ToUInt32(buffer, 0);
			if (magic == DocumentCompression.CompressFileMagic)
				return true;

			// If the file is uncompressed, we need to unread the first few bytes which we've already read.
			underlyingStream.Unread(buffer);
			return false;
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

		public override int Read(byte[] buffer, int offset, int count)
		{
			int read = 0;
			if (compressed)
				read = deflateStream.Read(buffer, offset, count);
			else
				read = underlyingStream.Read(buffer, offset, count);

			pos += read;
			return read;
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException();
		}

		public override void Flush()
		{
			throw new NotSupportedException();
		}

		public override long Position
		{
			get { return pos; }
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
			if (deflateStream != null)
				deflateStream.Close();
			underlyingStream.Close();
		}
	}
}
