using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using log4net;
using Raven.Database.Data;
using Raven.Database.Plugins;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
	public class WorkContext
	{
		private readonly ConcurrentQueue<ServerError> serverErrors = new ConcurrentQueue<ServerError>();
		private readonly object waitForWork = new object();
		private volatile bool doWork = true;
		private readonly ILog log = LogManager.GetLogger(typeof (WorkContext));
		private readonly ThreadLocal<bool> shouldNotifyOnWork = new ThreadLocal<bool>();
		private readonly ReaderWriterLockSlim readerWriterLockSlim = new ReaderWriterLockSlim();
        public IEnumerable<AbstractIndexUpdateTrigger> IndexUpdateTriggers{ get; set;}
		public IEnumerable<AbstractReadTrigger> ReadTriggers { get; set; }
		public PerformanceCounters PerformanceCounters { get; set; }
		public bool DoWork
		{
			get { return doWork; }
		}

		public IndexStorage IndexStorage { get; set; }

		public IndexDefinitionStorage IndexDefinitionStorage { get; set; }

		public ITransactionalStorage TransactionaStorage { get; set; }

		public ServerError[] Errors
		{
			get { return serverErrors.ToArray(); }
		}

		public void WaitForWork(TimeSpan timeout)
		{
			if (!doWork)
				return;
			lock (waitForWork)
			{
				Monitor.Wait(waitForWork, timeout);
			}
		}

		public void ShouldNotifyAboutWork()
		{
			shouldNotifyOnWork.Value = true;
		}

		public void NotifyAboutWork()
		{
			if (shouldNotifyOnWork.Value == false)
				return;
			shouldNotifyOnWork.Value = false;
			lock (waitForWork)
			{
				log.Debug("Notifying background workers about work");
				Monitor.PulseAll(waitForWork);
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
	}
}
