using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions
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
		private int readersLockCount;

	    private bool isWriteLockHeld;

		public AsyncReaderWriterLock()
		{
			readerReleaser = Task.FromResult(new Releaser(this, false));
			writerReleaser = Task.FromResult(new Releaser(this, true));
		}

	    public Task<Releaser> ReadLockAsync()
	    {
		    lock (waitingWriters)
		    {
			    if (!isWriteLockHeld && readersLockCount >= 0 && waitingWriters.Count == 0)
			    {
				    readersLockCount++;
				    return readerReleaser;
			    }
			    
				readersWaitCount++;
			    return waitingReaders.Task.ContinueWith(t => t.Result);
		    }
	    }

	    public Task<Releaser> WriteLockAsync()
	    {
			lock (waitingWriters)
			{
				if (readersLockCount == 0)
				{
					isWriteLockHeld = true;
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
				if (readersLockCount == 0)
				{
					isWriteLockHeld = true;
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
					readersLockCount = readersWaitCount;
					readersWaitCount = 0;
					waitingReaders = new TaskCompletionSource<Releaser>();
				}
				else 
				{
					isWriteLockHeld = false;
					readersLockCount = 0;
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
				if (readersLockCount > 0) readersLockCount--;

				if (!isWriteLockHeld && readersLockCount == 0 && waitingWriters.Count > 0)
				{
					isWriteLockHeld = true;
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
