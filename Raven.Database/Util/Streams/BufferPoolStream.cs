using System;
using System.IO;

namespace Raven.Database.Util.Streams
{
	public class BufferPoolStream : Stream
	{
		public const int MaxBufferSize = 1024*128;
		private const int InitialBufferSize = 1024 * 8;
		private int timeInSize;
		private int steps = 1;
		private byte[] internalBuffer;

		private byte[] InternalBuffer
		{
			get
			{
				if (internalBuffer == null)
					internalBuffer = bufferPool.TakeBuffer(InitialBufferSize);
				return internalBuffer;
			}
		}
		private readonly IBufferPool bufferPool;
		private int positionInBuffer;
		private readonly Stream stream;

		public BufferPoolStream(Stream stream, IBufferPool bufferPool)
		{
			this.stream = stream;
			this.bufferPool = bufferPool;
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
			get { throw new NotSupportedException(); }
		}


		public override long Position
		{
			get { throw new NotSupportedException(); }
			set { throw new NotSupportedException(); }
		}
		
		public override void Flush()
		{
			InternalFlush();
			stream.Flush();
		}

		private void InternalFlush()
		{
			if (positionInBuffer == 0)
				return;
			stream.Write(InternalBuffer, 0, positionInBuffer);
			positionInBuffer = 0;
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
			throw new NotSupportedException();
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			while (true)
			{
				var size = Math.Min(InternalBuffer.Length - positionInBuffer, count);
				if (size <= 0)
					return;

				Buffer.BlockCopy(buffer, offset, InternalBuffer, positionInBuffer, size);
				positionInBuffer += size;
				if (positionInBuffer == InternalBuffer.Length)
				{
					InternalFlush();
					AdjustBufferSize();
					offset += size;
					count -= size;
				}
				else
				{
					return;
				}
			}
		}

		private void AdjustBufferSize()
		{
			if (timeInSize++ < steps)
				return; 
			
			if (internalBuffer.Length >= MaxBufferSize)
				return;

			timeInSize = 0;
			steps += 2;
			var newSize = Math.Min(internalBuffer.Length*2, MaxBufferSize);
			bufferPool.ReturnBuffer(internalBuffer);
			internalBuffer = bufferPool.TakeBuffer(newSize);
		}

		private bool disposed;

		protected override void Dispose(bool disposing)
		{
			if (disposed)
				return;
			Flush();
			disposed = true;
			if(internalBuffer != null)
			{
				bufferPool.ReturnBuffer(internalBuffer);
			}
			base.Dispose(disposing);
			stream.Dispose();
		}
	}
}