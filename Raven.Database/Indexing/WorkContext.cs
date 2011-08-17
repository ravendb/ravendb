//-----------------------------------------------------------------------
// <copyright file="WorkContext.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Threading;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
	public class WorkContext : IDisposable
	{
		private readonly ConcurrentQueue<ServerError> serverErrors = new ConcurrentQueue<ServerError>();
		private readonly object waitForWork = new object();
		private volatile bool doWork = true;
		private int workCounter;
		private static readonly Logger log = LogManager.GetCurrentClassLogger();
		private readonly ThreadLocal<bool> shouldNotifyOnWork = new ThreadLocal<bool>();
		private readonly ReaderWriterLockSlim readerWriterLockSlim = new ReaderWriterLockSlim();
		public OrderedPartCollection<AbstractIndexUpdateTrigger> IndexUpdateTriggers { get; set; }
		public OrderedPartCollection<AbstractReadTrigger> ReadTriggers { get; set; }
		public bool DoWork
		{
			get { return doWork; }
		}

		public InMemoryRavenConfiguration Configuration { get; set; }
		public IndexStorage IndexStorage { get; set; }

		public IndexDefinitionStorage IndexDefinitionStorage { get; set; }

		public ITransactionalStorage TransactionaStorage { get; set; }

		public ServerError[] Errors
		{
			get { return serverErrors.ToArray(); }
		}

		public bool WaitForWork(TimeSpan timeout, ref int workerWorkCounter)
		{
			return WaitForWork(timeout, ref workerWorkCounter, null);
		}

		public bool WaitForWork(TimeSpan timeout, ref int workerWorkCounter, Action beforeWait)
		{
			if (!doWork)
				return false;
			var currentWorkCounter = Thread.VolatileRead(ref workCounter);
			if (currentWorkCounter != workerWorkCounter)
			{
				workerWorkCounter = currentWorkCounter;
				return true;
			}
			if (beforeWait != null)
				beforeWait();
			lock (waitForWork)
			{
				if (!doWork)
					return false;
				currentWorkCounter = Thread.VolatileRead(ref workCounter);
				if (currentWorkCounter != workerWorkCounter)
				{
					workerWorkCounter = currentWorkCounter;
					return true;
				}
				return Monitor.Wait(waitForWork, timeout);
			}
		}

		public void ShouldNotifyAboutWork()
		{
			shouldNotifyOnWork.Value = true;
		}

		public void HandleWorkNotifications()
		{
			if (shouldNotifyOnWork.Value == false)
				return;
			shouldNotifyOnWork.Value = false;
			NotifyAboutWork();
		}

		public void NotifyAboutWork()
		{
			Interlocked.Increment(ref workCounter);
			lock (waitForWork)
			{
				Monitor.PulseAll(waitForWork);
				Interlocked.Increment(ref workCounter);
			}
		}

		public void StartWork()
		{
			doWork = true;
		}

		public void StopWork()
		{
			log.Debug("Stopping background workers");
			doWork = false;
			lock (waitForWork)
			{
				Monitor.PulseAll(waitForWork);
			}
		}

		public void AddError(string index, string key, string error)
		{
			serverErrors.Enqueue(new ServerError
			{
				Document = key,
				Error = error,
				Index = index,
				Timestamp = DateTime.UtcNow
			});
			if (serverErrors.Count <= 50)
				return;
			ServerError ignored;
			serverErrors.TryDequeue(out ignored);
		}

		public IDisposable ExecutingWork()
		{
			readerWriterLockSlim.EnterReadLock();
			return new DisposableAction(readerWriterLockSlim.ExitReadLock);
		}

		public IDisposable HaltAllWork()
		{
			readerWriterLockSlim.EnterWriteLock();
			return new DisposableAction(readerWriterLockSlim.ExitWriteLock);
		}

	    public void Dispose()
	    {
	        shouldNotifyOnWork.Dispose();
	    }
	}
}
