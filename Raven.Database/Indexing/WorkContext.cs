//-----------------------------------------------------------------------
// <copyright file="WorkContext.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using NLog;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using System.Linq;

namespace Raven.Database.Indexing
{
	public class WorkContext : IDisposable
	{
		private readonly ConcurrentQueue<ServerError> serverErrors = new ConcurrentQueue<ServerError>();
		private readonly object waitForWork = new object();
		private volatile bool doWork = true;
		private int workCounter;
		private static readonly Logger log = LogManager.GetCurrentClassLogger();
		private readonly ThreadLocal<List<Func<string>>> shouldNotifyOnWork = new ThreadLocal<List<Func<string>>>(() => new List<Func<string>>());
		public OrderedPartCollection<AbstractIndexUpdateTrigger> IndexUpdateTriggers { get; set; }
		public OrderedPartCollection<AbstractReadTrigger> ReadTriggers { get; set; }

		public DateTime LastWorkTime { get; private set; }

		public bool DoWork
		{
			get { return doWork; }
		}

		public void UpdateFoundWork()
		{
			LastWorkTime = SystemTime.UtcNow;
		}

		public InMemoryRavenConfiguration Configuration { get; set; }
		public IndexStorage IndexStorage { get; set; }

		public IndexDefinitionStorage IndexDefinitionStorage { get; set; }

		public ITransactionalStorage TransactionaStorage { get; set; }

		public ServerError[] Errors
		{
			get { return serverErrors.ToArray(); }
		}

		public int CurrentNumberOfItemsToIndexInSingleBatch { get; set; }

		public int CurrentNumberOfItemsToReduceInSingleBatch { get; set; }

		public bool WaitForWork(TimeSpan timeout, ref int workerWorkCounter, string name)
		{
			return WaitForWork(timeout, ref workerWorkCounter, null, name);
		}

		public bool WaitForWork(TimeSpan timeout, ref int workerWorkCounter, Action beforeWait, string name)
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
				log.Debug("No work was found, workerWorkCounter: {0}, for: {1}, will wait for additional work", workerWorkCounter, name);
				var forWork = Monitor.Wait(waitForWork, timeout);
				if (forWork)
					LastWorkTime = SystemTime.UtcNow;
				return forWork;
			}
		}

		public void ShouldNotifyAboutWork(Func<string> why)
		{
			shouldNotifyOnWork.Value.Add(why);
			UpdateFoundWork();
		}

		public void HandleWorkNotifications()
		{
			if (shouldNotifyOnWork.Value.Count == 0)
				return;
			NotifyAboutWork();
		}

		public void NotifyAboutWork()
		{
			lock (waitForWork)
			{
				if (doWork == false)
					return;
				var increment = Interlocked.Increment(ref workCounter);
				if (log.IsDebugEnabled)
				{
					var reason = string.Join(", ", shouldNotifyOnWork.Value.Select(action => action()).Where(x => x != null));
					log.Debug("Incremented work counter to {0} because: {1}", increment, reason);
				}
				shouldNotifyOnWork.Value.Clear();
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
				Timestamp = SystemTime.UtcNow
			});
			if (serverErrors.Count <= 50)
				return;
			ServerError ignored;
			serverErrors.TryDequeue(out ignored);
		}


		public void Dispose()
		{
			shouldNotifyOnWork.Dispose();
		}

		public void ClearErrorsFor(string name)
		{
			var list = new List<ServerError>();

			ServerError error;
			while (serverErrors.TryDequeue(out error))
			{
				if (StringComparer.InvariantCultureIgnoreCase.Equals(error.Index, name) == false)
					list.Add(error);
			}

			foreach (var serverError in list)
			{
				serverErrors.Enqueue(serverError);
			}
		}
	}
}
