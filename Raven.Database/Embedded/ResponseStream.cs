using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;

namespace Raven.Database.Embedded
{
	internal class ResponseStream : Stream
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private readonly SemaphoreSlim tryTakeDataLock = new SemaphoreSlim(1, 1);
		private readonly CancellationTokenSource abort = new CancellationTokenSource();
		private readonly BlockingCollection<byte[]> blocks = new BlockingCollection<byte[]>();
		private readonly Guid id = Guid.NewGuid();

		private volatile bool disposed;
		private readonly Action onFirstWrite;
		private bool firstWrite = true;
		private readonly bool enableLogging;
		private byte[] currentBlock;
		private int currentBlockIndex;
		private Exception abortException;

		internal ResponseStream(Action onFirstWrite, bool enableLogging)
		{
			if (onFirstWrite == null)
				throw new ArgumentNullException("onFirstWrite");
			
			this.onFirstWrite = onFirstWrite;
			this.enableLogging = enableLogging;
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
			get { return true; }
		}

		#region NotSupported

		public override long Length
		{
			get { throw new NotSupportedException(); }
		}

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

		#endregion NotSupported

		public override void Flush()
		{
			if (disposed) // check disposed
			{
				throw new ObjectDisposedException(GetType().FullName);
			}

			FirstWrite();
		}

		public override Task FlushAsync(CancellationToken cancellationToken)
		{
			if (cancellationToken.IsCancellationRequested)
			{
				var tcs = new TaskCompletionSource<object>();
				tcs.TrySetCanceled();
				return tcs.Task;
			}

			Flush();

			return new CompletedTask();
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			VerifyBuffer(buffer, offset, count, allowEmpty: false);

			var bytesRead = 0;

			while (true)
			{
				try
				{
					abort.Token.ThrowIfCancellationRequested();

					if (currentBlock != null)
					{
						int copy = Math.Min(count - bytesRead, currentBlock.Length - currentBlockIndex);
						Buffer.BlockCopy(currentBlock, currentBlockIndex, buffer, offset + bytesRead, copy);
						currentBlockIndex += copy;
						bytesRead += copy;

						if (currentBlock.Length <= currentBlockIndex)
						{
							currentBlock = null;
							currentBlockIndex = 0;
						}

						try
						{
							if (bytesRead == count || blocks.Count == 0)
								return bytesRead;
						}
						catch (ObjectDisposedException)
						{
							return bytesRead;
						}
					}

					tryTakeDataLock.Wait();
					try
					{
						if (disposed)
							return bytesRead;

						if (blocks.TryTake(out currentBlock, Timeout.Infinite, abort.Token) == false)
							return bytesRead;
					}
					finally
					{
						tryTakeDataLock.Release();
					}
				}
				catch (OperationCanceledException)
				{
					throw new IOException("Read operation has been aborted", abortException);
				}
			}
		}

		public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
		{
			VerifyBuffer(buffer, offset, count, allowEmpty: false);

			var linkedCancelToken = CancellationTokenSource.CreateLinkedTokenSource(abort.Token, cancellationToken).Token;

			await tryTakeDataLock.WaitAsync(linkedCancelToken);
			try
			{
				var bytesRead = 0;

				while (true)
				{
					try
					{
						abort.Token.ThrowIfCancellationRequested();

						if (currentBlock != null)
						{
							int copy = Math.Min(count - bytesRead, currentBlock.Length - currentBlockIndex);
							Buffer.BlockCopy(currentBlock, currentBlockIndex, buffer, offset + bytesRead, copy);
							currentBlockIndex += copy;
							bytesRead += copy;

							if (currentBlock.Length <= currentBlockIndex)
							{
								currentBlock = null;
								currentBlockIndex = 0;
							}

							try
							{
								if (bytesRead == count || blocks.Count == 0)
									return bytesRead;
							}
							catch (ObjectDisposedException)
							{
								return bytesRead;
							}
						}

						await tryTakeDataLock.WaitAsync(linkedCancelToken).ConfigureAwait(false);
						try
						{
							if (disposed)
								return bytesRead;

							if (blocks.TryTake(out currentBlock, Timeout.Infinite, linkedCancelToken) == false)
								return bytesRead;
						}
						finally
						{
							tryTakeDataLock.Release();
						}
					}
					catch (OperationCanceledException)
					{
						if (abort.IsCancellationRequested)
							throw new IOException("Read operation has been aborted", abortException);

						throw;
					}
				}
			}
			finally
			{
				tryTakeDataLock.Release();
			}
		}

		// Write with count 0 will still trigger OnFirstWrite
		public override void Write(byte[] buffer, int offset, int count)
		{
			VerifyBuffer(buffer, offset, count, allowEmpty: true);

			if (count == 0)
			{
				FirstWrite();
				return;
			}

			var newBuf = new byte[count];
			Buffer.BlockCopy(buffer, offset, newBuf, 0, count);

			if (enableLogging)
				log.Info("ResponseStream ({0}). Write. Content: {1}", id, Encoding.UTF8.GetString(newBuf));

			blocks.Add(newBuf);

			FirstWrite();
		}

		public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
		{
			Write(buffer, offset, count);
			var tcs = new TaskCompletionSource<object>(state);
			tcs.TrySetResult(null);
			IAsyncResult result = tcs.Task;
			if (callback != null)
			{
				callback(result);
			}
			return result;
		}

		public override void EndWrite(IAsyncResult asyncResult)
		{
		}

		public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
		{
			VerifyBuffer(buffer, offset, count, allowEmpty: true);
			if (cancellationToken.IsCancellationRequested)
			{
				var tcs = new TaskCompletionSource<object>();
				tcs.TrySetCanceled();
				return tcs.Task;
			}

			Write(buffer, offset, count);
			return new CompletedTask();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private void FirstWrite()
		{
			if (firstWrite)
			{
				firstWrite = false;
				onFirstWrite();
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static void VerifyBuffer(byte[] buffer, int offset, int count, bool allowEmpty)
		{
			if (buffer == null)
			{
				throw new ArgumentNullException("buffer");
			}
			if (offset < 0 || offset > buffer.Length)
			{
				throw new ArgumentOutOfRangeException("offset", offset, string.Empty);
			}
			if (count < 0 || count > buffer.Length - offset
				|| (!allowEmpty && count == 0))
			{
				throw new ArgumentOutOfRangeException("count", count, string.Empty);
			}
		}

		protected override void Dispose(bool disposing)
		{
			if (disposed)
				return;

			blocks.CompleteAdding();

			base.Dispose(disposing);

			if (disposing == false || blocks.IsCompleted == false) // do not dispose until the collection of blocks is empty
				return;

			tryTakeDataLock.Wait();
			try
			{
				blocks.Dispose();
				disposed = true;
			}
			finally
			{
				tryTakeDataLock.Release();
			}
		}

		internal void Abort(Exception exception)
		{
			abortException = exception;
			abort.Cancel();
		}
	}
}
