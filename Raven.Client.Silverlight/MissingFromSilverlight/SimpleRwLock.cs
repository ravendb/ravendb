using System.Threading;

namespace System.Collections.Concurrent
{
	internal class SimpleRwLock
	{
		const int RwWait = 1;
		const int RwWrite = 2;
		const int RwRead = 4;

		int rwlock;

		public void EnterReadLock()
		{
			SpinWait sw = new SpinWait();
			do
			{
				while ((rwlock & (RwWrite | RwWait)) > 0)
					sw.SpinOnce();

				if ((Interlocked.Add(ref rwlock, RwRead) & (RwWait | RwWait)) == 0)
					return;

				Interlocked.Add(ref rwlock, -RwRead);
			} while (true);
		}

		public void ExitReadLock()
		{
			Interlocked.Add(ref rwlock, -RwRead);
		}

		public void EnterWriteLock()
		{
			SpinWait sw = new SpinWait();
			do
			{
				int state = rwlock;
				if (state < RwWrite)
				{
					if (Interlocked.CompareExchange(ref rwlock, RwWrite, state) == state)
						return;
					state = rwlock;
				}
				// We register our interest in taking the Write lock (if upgradeable it's already done)
				while ((state & RwWait) == 0 && Interlocked.CompareExchange(ref rwlock, state | RwWait, state) != state)
					state = rwlock;
				// Before falling to sleep
				while (rwlock > RwWait)
					sw.SpinOnce();
			} while (true);
		}

		public void ExitWriteLock()
		{
			Interlocked.Add(ref rwlock, -RwWrite);
		}
	}
}