using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Database.Util.Streams
{
	//adapted from http://stackoverflow.com/questions/3721552/implementing-async-stream-for-producer-cosumer-in-c-sharp-net
	public class BlockingStream : Stream
	{
		private readonly CancellationToken _cancellationToken;
		private readonly BlockingCollection<byte[]> _blocks;
		private byte[] _currentBlock;
		private int _currentBlockIndex;
		private bool _isDisposed;

		public BlockingStream(CancellationToken cancellationToken)
		{
			_isDisposed = false;
			_cancellationToken = cancellationToken;
			_blocks = new BlockingCollection<byte[]>();
		}

		public override bool CanTimeout { get { return false; } }
		public override bool CanRead { get { return true; } }
		public override bool CanSeek { get { return false; } }
		public override bool CanWrite { get { return true; } }
		public override long Length { get { throw new NotSupportedException(); } }
		public override void Flush() { }
		public long TotalBytesWritten { get; private set; }
		public int WriteCount { get; private set; }

		public override long Position
		{
			get { throw new NotSupportedException(); }
			set { throw new NotSupportedException(); }
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
			if (_isDisposed || _cancellationToken.IsCancellationRequested)
			{
				Dispose(true);
				return 0;
			}


			ValidateBufferArgs(buffer, offset, count);

			int bytesRead = 0;
			while (true)
			{
				if (_currentBlock != null)
				{
					int copy = Math.Min(count - bytesRead, _currentBlock.Length - _currentBlockIndex);
					Array.Copy(_currentBlock, _currentBlockIndex, buffer, offset + bytesRead, copy);
					_currentBlockIndex += copy;
					bytesRead += copy;

					if (_currentBlock.Length <= _currentBlockIndex)
					{
						_currentBlock = null;
						_currentBlockIndex = 0;
					}

					if (bytesRead == count)
						return bytesRead;
				}

				if (!_blocks.TryTake(out _currentBlock, 250))
					return bytesRead;
			}
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			if (_isDisposed || _cancellationToken.IsCancellationRequested)
			{
				Dispose(true);
				return;
			}

			ValidateBufferArgs(buffer, offset, count);

			var newBuf = new byte[count];
			Array.Copy(buffer, offset, newBuf, 0, count);
			_blocks.Add(newBuf, _cancellationToken);
			TotalBytesWritten += count;
			WriteCount++;
		}

		protected override void Dispose(bool disposing)
		{
			if (!_isDisposed)
			{
				base.Dispose(disposing);
				if (disposing)
				{
					_blocks.Dispose();
					_isDisposed = true;
				}
			}
		}

		public override void Close()
		{
			CompleteWriting();
			base.Close();
		}

		public void CompleteWriting()
		{
			if (!_isDisposed)
			{
				try
				{
					_blocks.CompleteAdding();
				}
				catch (ObjectDisposedException)
				{
				}
			}
		}

		private static void ValidateBufferArgs(byte[] buffer, int offset, int count)
		{
			if (buffer == null)
				throw new ArgumentNullException("buffer");
			if (offset < 0)
				throw new ArgumentOutOfRangeException("offset");
			if (count < 0)
				throw new ArgumentOutOfRangeException("count");
			if (buffer.Length - offset < count)
				throw new ArgumentException("buffer.Length - offset < count");
		}
	}
}


