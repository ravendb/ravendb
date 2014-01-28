using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Abstractions.Util
{
	/// <summary>
	/// with some adaptations taken from blog post http://blogs.msdn.com/b/pfxteam/archive/2012/02/12/10267069.aspx
	/// </summary>
	/// <remarks>
	/// Should allow single write lock at any time, and multiple concurrent read locks. 
	/// When read lock is released and there is a waiting writing lock - it is given a precedence over read locks
	/// </remarks>
    public class AsyncReaderWriterLock
	{
		private readonly Task<Releaser> readerReleaser;
		private readonly Task<Releaser> writerReleaser;

		private readonly Queue<TaskCompletionSource<Releaser>> waitingWriters = new Queue<TaskCompletionSource<Releaser>>();
		private TaskCompletionSource<Releaser> waitingReaders = new TaskCompletionSource<Releaser>();

		private int readersWaitCount;

		public AsyncReaderWriterLock()
		{
#if !SILVERLIGHT			
			readerReleaser = Task.FromResult(new Releaser(this, false));
			writerReleaser = Task.FromResult(new Releaser(this, true));
#else
			readerReleaser = CompletedTask.With(new Releaser(this, false));
			writerReleaser = CompletedTask.With(new Releaser(this, true));
#endif
		}

		public int ReadersLockCount { get; private set; }

		public bool IsWriteLockHeld { get; private set; }

		public Task<Releaser> ReadLockAsync()
	    {
		    lock (waitingWriters)
		    {
			    if (!IsWriteLockHeld && ReadersLockCount >= 0 && waitingWriters.Count == 0)
			    {
				    ReadersLockCount++;
				    return readerReleaser;
			    }
			    
				readersWaitCount++;
			    return waitingReaders.Task.ContinueWith(t => t.Result);
		    }
	    }

		public Releaser ReadLock()
		{
			var releaserTask = ReadLockAsync();
			releaserTask.Wait();
			return releaserTask.Result;
		}

	    public Task<Releaser> WriteLockAsync()
	    {
			lock (waitingWriters)
			{
				if (ReadersLockCount == 0)
				{
					IsWriteLockHeld = true;
					return writerReleaser;
				}
				
				var waiter = new TaskCompletionSource<Releaser>();
				waitingWriters.Enqueue(waiter);
				return waiter.Task;
			} 		    
	    }

		public Releaser WriteLock()
		{
			lock (waitingWriters)
			{
				if (ReadersLockCount == 0)
				{
					IsWriteLockHeld = true;
					writerReleaser.Wait();
					
					return writerReleaser.Result;
				}

				var waiter = new TaskCompletionSource<Releaser>();
				waitingWriters.Enqueue(waiter);
				var waiterTask = waiter.Task;
				waiterTask.Wait();

				return waiterTask.Result;
			}
		}

		private void ReleaseWriteLock()
		{
			TaskCompletionSource<Releaser> readLocksToAcquire = null;
			bool shouldAcquireWriteLock = false;

			lock (waitingWriters)
			{
				if (waitingWriters.Count > 0) //if writing locks are waiting - let one of them through
				{
					readLocksToAcquire = waitingWriters.Dequeue();
					shouldAcquireWriteLock = true;
				}
				else if (readersWaitCount > 0)//if no waiting writing locks - let all waiting read locks through
				{
					readLocksToAcquire = waitingReaders;
					ReadersLockCount = readersWaitCount;
					readersWaitCount = 0;
					waitingReaders = new TaskCompletionSource<Releaser>();
				}
				else 
				{
					IsWriteLockHeld = false;
					ReadersLockCount = 0;
				}
			}
			
			if (readLocksToAcquire != null)
				readLocksToAcquire.SetResult(new Releaser(this, shouldAcquireWriteLock)); 			
		}

		private void ReleaseReadLock()
		{
			TaskCompletionSource<Releaser> writeLockToAquire = null;

			lock (waitingWriters)
			{
				if (ReadersLockCount > 0) ReadersLockCount--;

				if (!IsWriteLockHeld && ReadersLockCount == 0 && waitingWriters.Count > 0)
				{
					IsWriteLockHeld = true;
					writeLockToAquire = waitingWriters.Dequeue();
				}
			}

			if (writeLockToAquire != null)
				writeLockToAquire.SetResult(new Releaser(this, true)); 			
		}

		public struct Releaser : IDisposable
		{
			private readonly AsyncReaderWriterLock lockToRelease;
			private readonly bool isWriter;

			internal Releaser(AsyncReaderWriterLock lockToRelease, bool writer)
			{
				this.lockToRelease = lockToRelease;
				isWriter = writer;
			}

			public void Dispose()
			{
				if (lockToRelease != null)
				{
					if (isWriter) lockToRelease.ReleaseWriteLock();
					else lockToRelease.ReleaseReadLock();
				}
			}
		}
	}
}
