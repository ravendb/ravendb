//-----------------------------------------------------------------------
// <copyright file="WorkContext.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security;
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
		private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
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
				{
					// need to clear this anyway
					shouldNotifyOnWork.Value.Clear();
					return;
				}
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

		public void StopWorkRude()
		{
			StopWork();
			cancellationTokenSource.Cancel();
		}

		public CancellationToken CancellationToken
		{
			get { return cancellationTokenSource.Token; }
		}

		public void Dispose()
		{
			shouldNotifyOnWork.Dispose();
			if (DocsPerSecCounter != null)
				DocsPerSecCounter.Dispose();
			if (ReducedPerSecCounter != null)
				ReducedPerSecCounter.Dispose();
			if (IndexedPerSecCounter != null)
				IndexedPerSecCounter.Dispose();
			cancellationTokenSource.Dispose();
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

		public Action<IndexChangeNotification> RaiseIndexChangeNotification { get; set; }

		private PerformanceCounter DocsPerSecCounter { get; set; }
		private PerformanceCounter IndexedPerSecCounter { get; set; }
		private PerformanceCounter ReducedPerSecCounter { get; set; }
		private bool useCounters = true;

		public void DocsPerSecIncreaseBy(int numOfDocs)
		{
			if (useCounters)
			{
				DocsPerSecCounter.IncrementBy(numOfDocs);
			}
		}
		public void IndexedPerSecIncreaseBy(int numOfDocs)
		{
			if (useCounters)
			{
				IndexedPerSecCounter.IncrementBy(numOfDocs);
			}
		}
		public void ReducedPerSecIncreaseBy(int numOfDocs)
		{
			if (useCounters)
			{
				ReducedPerSecCounter.IncrementBy(numOfDocs);
			}
		}

		private void CreatePreformanceCounters(string name)
		{
			DocsPerSecCounter = new PerformanceCounter
			{
				CategoryName = "RavenDB-" + name,
				CounterName = "# docs / sec",
				MachineName = ".",
				ReadOnly = false
			};

			IndexedPerSecCounter = new PerformanceCounter
			{
				CategoryName = "RavenDB-" + name,
				CounterName = "# docs indexed / sec",
				MachineName = ".",
				ReadOnly = false
			};

			ReducedPerSecCounter = new PerformanceCounter
			{
				CategoryName = "RavenDB-" + name,
				CounterName = "# docs reduced / sec",
				MachineName = ".",
				ReadOnly = false
			};
		}

		private void SetupPreformanceCounter(string name)
		{
			if (PerformanceCounterCategory.Exists("RavenDB-" + name))
				return;

			var counters = new CounterCreationDataCollection();

			// 1. counter for counting operations per second:
			//        PerformanceCounterType.RateOfCountsPerSecond32
			var docsPerSecond = new CounterCreationData
			{
				CounterName = "# docs / sec",
				CounterHelp = "Number of documents added per second",
				CounterType = PerformanceCounterType.RateOfCountsPerSecond32
			};
			counters.Add(docsPerSecond);

			// 2. counter for counting operations per second:
			//        PerformanceCounterType.RateOfCountsPerSecond32
			var indexedPerSecond = new CounterCreationData
			{
				CounterName = "# docs indexed / sec",
				CounterHelp = "Number of documents indexed per second",
				CounterType = PerformanceCounterType.RateOfCountsPerSecond32
			};
			counters.Add(indexedPerSecond);

			// 3. counter for counting operations per second:
			//        PerformanceCounterType.RateOfCountsPerSecond32
			var reducesPerSecond = new CounterCreationData
			{
				CounterName = "# docs reduced / sec",
				CounterHelp = "Number of documents reduced per second",
				CounterType = PerformanceCounterType.RateOfCountsPerSecond32
			};
			counters.Add(reducesPerSecond);

			// create new category with the counters above

			PerformanceCounterCategory.Create("RavenDB-" + name,
											  "RevenDB category", PerformanceCounterCategoryType.Unknown, counters);
		}

		public void Init(string name)
		{
			if (Configuration.RunInMemory)
			{
				useCounters = false;
				return;
			}


			name = name ?? Constants.SystemDatabase;
			try
			{
				SetupPreformanceCounter(name);
				CreatePreformanceCounters(name);
			}
			catch(UnauthorizedAccessException e)
			{
				log.WarnException(
					"Could not setup performance counters properly because of access permissions, perf counters will not be used", e);
				useCounters = false;
			}
			catch (SecurityException e)
			{
				log.WarnException(
					"Could not setup performance counters properly because of access permissions, perf counters will not be used", e);
				useCounters = false;
			}
		}
	}
}