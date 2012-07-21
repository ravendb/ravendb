using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Bundles.Compression.Streams
{
	internal class StreamReaderWithUnread
	{
		public readonly Stream stream;
		private readonly Stack<Buffer> unreadBuffer = new Stack<Buffer>();

		public StreamReaderWithUnread(Stream stream)
		{
			this.stream = stream;
		}

		public int Read(byte[] buffer, int start, int length)
		{
			if (unreadBuffer.Count != 0)
			{
				var next = unreadBuffer.Pop();
				if (next.Length > length)
				{
					Array.Copy(next.Data, next.Start, buffer, start, length);
					unreadBuffer.Push(next.Skip(length));
					return length;
				}
				else
				{
					Array.Copy(next.Data, next.Start, buffer, start, next.Length);
					return next.Length;
				}
			}

			return stream.Read(buffer, start, length);
		}

		public void Unread(IEnumerable<byte> data)
		{
			unreadBuffer.Push(new Buffer(data));
		}

		public void Close()
		{
			stream.Close();
		}

		private struct Buffer
		{
			public readonly byte[] Data;
			public readonly int Start;
			public readonly int Length;

			public Buffer(IEnumerable<byte> data)
			{
				this.Data = data.ToArray();
				this.Start = 0;
				this.Length = this.Data.Length;
			}

			public Buffer(byte[] data, int start, int length)
			{
				this.Data = data;
				this.Start = start;
				this.Length = length;
			}

			public Buffer Skip(int count)
			{
				return new Buffer(Data, Start + count, Length - count);
			}
		}
	}
}
