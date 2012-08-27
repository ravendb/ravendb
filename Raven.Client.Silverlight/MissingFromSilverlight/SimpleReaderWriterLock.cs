// -----------------------------------------------------------------------
//  <copyright file="SimpleReaderWriterLock.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;

namespace Raven.Client.Silverlight.MissingFromSilverlight
{
	public class SimpleReaderWriterLock
	{
		private readonly object internalLock = new object();
		private int activeReaders = 0;
		private bool activeWriter = false;

		public void EnterReadLock()
		{
			lock (internalLock)
			{
				while (activeWriter)
					Monitor.Wait(internalLock);
				++activeReaders;
			}
		}

		public void ExitReadLock()
		{
			lock (internalLock)
			{
				// if activeReaders <= 0 do some error handling
				--activeReaders;
				Monitor.PulseAll(internalLock);
			}
		}

		public void EnterWriteLock()
		{
			lock (internalLock)
			{
				while (activeWriter)
					Monitor.Wait(internalLock);

				activeWriter = true;

				while (activeReaders > 0)
					Monitor.Wait(internalLock);
			}
		}

		public void ExitWriteLock()
		{
			lock (internalLock)
			{
				activeWriter = false;
				Monitor.PulseAll(internalLock);
			}
		}
	}
}