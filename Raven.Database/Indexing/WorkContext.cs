using System;
using System.Collections.Concurrent;
using System.Threading;
using log4net;
using Raven.Database.Data;
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

		public bool DoWork
		{
			get { return doWork; }
		}

		public IndexStorage IndexStorage { get; set; }

		public IndexDefinitionStorage IndexDefinitionStorage { get; set; }

		public TransactionalStorage TransactionaStorage { get; set; }

		public ServerError[] Errors
		{
			get { return serverErrors.ToArray(); }
		}

		public void WaitForWork()
		{
			if (!doWork)
				return;
			lock (waitForWork)
			{
				Monitor.Wait(waitForWork, TimeSpan.FromSeconds(1));
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
				Timestamp = DateTime.Now
			});
			if (serverErrors.Count <= 50)
				return;
			ServerError ignored;
			serverErrors.TryDequeue(out ignored);
		}
	}
}