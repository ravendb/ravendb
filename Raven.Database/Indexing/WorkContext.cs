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
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Raven.Database.Server.Responders.Debugging;
using Raven.Database.Storage;
using System.Linq;
using Raven.Database.Util;

namespace Raven.Database.Indexing
{
	public class WorkContext : IDisposable
	{
		private readonly ConcurrentSet<FutureBatchStats> futureBatchStats = new ConcurrentSet<FutureBatchStats>();

		private readonly SizeLimitedConcurrentSet<string> recentlyDeleted = new SizeLimitedConcurrentSet<string>(100, StringComparer.OrdinalIgnoreCase);

		private readonly SizeLimitedConcurrentSet<ActualIndexingBatchSize> lastActualIndexingBatchSize = new SizeLimitedConcurrentSet<ActualIndexingBatchSize>(25);
		private readonly ConcurrentQueue<ServerError> serverErrors = new ConcurrentQueue<ServerError>();
		private readonly object waitForWork = new object();
		private volatile bool doWork = true;
		private volatile bool doIndexing = true;
		private int workCounter;
		private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
		private static readonly ILog log = LogManager.GetCurrentClassLogger();
		private readonly ThreadLocal<List<Func<string>>> shouldNotifyOnWork = new ThreadLocal<List<Func<string>>>(() => new List<Func<string>>());

	    public WorkContext()
	    {
	        DoNotTouchAgainIfMissingReferences = new ConcurrentDictionary<int, ConcurrentSet<string>>();
            CurrentlyRunningQueries = new ConcurrentDictionary<string, ConcurrentSet<ExecutingQueryInfo>>(StringComparer.OrdinalIgnoreCase);
	    }

		public OrderedPartCollection<AbstractIndexUpdateTrigger> IndexUpdateTriggers { get; set; }
		public OrderedPartCollection<AbstractReadTrigger> ReadTriggers { get; set; }
        public OrderedPartCollection<AbstractIndexReaderWarmer> IndexReaderWarmers { get; set; }
		public string DatabaseName { get; set; }

		public DateTime LastWorkTime { get; private set; }

		public bool DoWork
		{
			get { return doWork; }
		}

		public bool RunIndexing
		{
			get { return doWork && doIndexing; }
		}

		public void UpdateFoundWork()
		{
			LastWorkTime = SystemTime.UtcNow;
		}

        //collection that holds information about currently running queries, in the form of [Index name -> (When query started,IndexQuery data)]
        public ConcurrentDictionary<string,ConcurrentSet<ExecutingQueryInfo>> CurrentlyRunningQueries { get; private set; }

		public InMemoryRavenConfiguration Configuration { get; set; }
		public IndexStorage IndexStorage { get; set; }

		public TaskScheduler TaskScheduler { get; set; }
		public IndexDefinitionStorage IndexDefinitionStorage { get; set; }

		public ITransactionalStorage TransactionalStorage { get; set; }

		public ServerError[] Errors
		{
			get { return serverErrors.ToArray(); }
		}

		public int CurrentNumberOfItemsToIndexInSingleBatch { get; set; }

		public int CurrentNumberOfItemsToReduceInSingleBatch { get; set; }

		public int NumberOfItemsToExecuteReduceInSingleStep
		{
			get { return Configuration.NumberOfItemsToExecuteReduceInSingleStep; }
		}

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
                CancellationToken.ThrowIfCancellationRequested();
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
			if (disposed)
				return;
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
					if(disposed == false)
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
			doIndexing = true;
		}

		public void StopWork()
		{
			log.Debug("Stopping background workers");
			doWork = false;
			doIndexing = false;
			lock (waitForWork)
			{
				Monitor.PulseAll(waitForWork);
			}
		}

		public void AddError(int index, string indexName, string key, string error )
		{
            AddError(index, indexName, key, "Unknown", "Unknown");
		}

		public void AddError(int index, string indexName, string key, string error, string component)
		{
			serverErrors.Enqueue(new ServerError
			{
				Document = key,
				Error = error,
				Index = index,
                IndexName = indexName,
                Action = component,
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
			disposed = true;

			shouldNotifyOnWork.Dispose();

			if (DocsPerSecCounter != null)
				DocsPerSecCounter.Dispose();
			if (ReducedPerSecCounter != null)
				ReducedPerSecCounter.Dispose();
			if (RequestsPerSecCounter != null)
				RequestsPerSecCounter.Dispose();
			if (ConcurrentRequestsCounter != null)
				ConcurrentRequestsCounter.Dispose();
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
				if (StringComparer.OrdinalIgnoreCase.Equals(error.Index, name) == false)
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
		private PerformanceCounter RequestsPerSecCounter { get; set; }
		private PerformanceCounter ConcurrentRequestsCounter { get; set; }
		private bool useCounters = true;
		private bool disposed;

		public float RequestsPerSecond
		{
			get
			{
				if (useCounters == false)
					return -1;
				return RequestsPerSecCounter.NextValue();
			}
		}

		public int ConcurrentRequests
		{
			get
			{
				if (useCounters == false)
					return -1;
				return (int)ConcurrentRequestsCounter.NextValue();
			}
		}

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

		public void IncrementRequestsPerSecCounter()
		{
			if (useCounters)
			{
				RequestsPerSecCounter.Increment();
			}
		}


		public void IncrementConcurrentRequestsCounter()
		{
			if (useCounters)
			{
				ConcurrentRequestsCounter.Increment();
			}
		}

		public void DecrementConcurrentRequestsCounter()
		{
			if (useCounters)
			{
				ConcurrentRequestsCounter.Decrement();
			}
		}

		private void SetupPerformanceCounter(string name)
		{
			const string categoryName = "RavenDB 2.0";
			var instances = new Dictionary<string, PerformanceCounterType>
			{
				{"# docs / sec", PerformanceCounterType.RateOfCountsPerSecond32},
				{"# docs indexed / sec", PerformanceCounterType.RateOfCountsPerSecond32}, 
				{"# docs reduced / sec", PerformanceCounterType.RateOfCountsPerSecond32},
				{"# req / sec", PerformanceCounterType.RateOfCountsPerSecond32}, 
				{"# of concurrent requests", PerformanceCounterType.NumberOfItems32}
			};

			if (IsValidCategory(categoryName, instances, name) == false)
			{
				var counterCreationDataCollection = new CounterCreationDataCollection();
				foreach (var instance in instances)
				{
					counterCreationDataCollection.Add(new CounterCreationData
					{
						CounterName = instance.Key,
						CounterType = instance.Value
					});
				}

				PerformanceCounterCategory.Create(categoryName, "RavenDB Performance Counters", PerformanceCounterCategoryType.MultiInstance, counterCreationDataCollection);
				PerformanceCounter.CloseSharedResources(); // http://blog.dezfowler.com/2007/08/net-performance-counter-problems.html
			}

			DocsPerSecCounter = new PerformanceCounter(categoryName, "# docs / sec", name, false);
			IndexedPerSecCounter = new PerformanceCounter(categoryName, "# docs indexed / sec", name, false);
			ReducedPerSecCounter = new PerformanceCounter(categoryName, "# docs reduced / sec", name, false);
			RequestsPerSecCounter = new PerformanceCounter(categoryName, "# req / sec", name, false);
			ConcurrentRequestsCounter = new PerformanceCounter(categoryName, "# of concurrent requests", name, false);
		}

		private bool IsValidCategory(string categoryName, Dictionary<string, PerformanceCounterType> instances, string instanceName)
		{
			if (PerformanceCounterCategory.Exists(categoryName) == false)
				return false;
			foreach (var performanceCounterType in instances)
			{
				try
				{
					new PerformanceCounter(categoryName, performanceCounterType.Key, instanceName, readOnly: true).Dispose();
				}
				catch (Exception)
				{
					PerformanceCounterCategory.Delete(categoryName);
					return false;
				}
			}
			return true;
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
				SetupPerformanceCounter(GetPerformanceCounterName(name));
			}
			catch (UnauthorizedAccessException e)
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

		private string GetPerformanceCounterName(string name)
		{
			//dealing with names who are very long (there is a limit of 80 chars for counter name)
			return name.Length > 70 ? name.Remove(70) : name;
		}

		public void ReportIndexingActualBatchSize(int size)
		{
			lastActualIndexingBatchSize.Add(new ActualIndexingBatchSize
			{
				Size = size,
				Timestamp = SystemTime.UtcNow
			});
		}

		public ConcurrentSet<FutureBatchStats> FutureBatchStats
		{
			get { return futureBatchStats; }
		}

		public SizeLimitedConcurrentSet<ActualIndexingBatchSize> LastActualIndexingBatchSize
		{
			get { return lastActualIndexingBatchSize; }
		}

		public DocumentDatabase Database { get; set; }
        public ConcurrentDictionary<int, ConcurrentSet<string>> DoNotTouchAgainIfMissingReferences { get; private set; }

	    public void AddFutureBatch(FutureBatchStats futureBatchStat)
		{
			futureBatchStats.Add(futureBatchStat);
			if (futureBatchStats.Count <= 30)
				return;

			foreach (var source in futureBatchStats.OrderBy(x => x.Timestamp).Take(5))
			{
				futureBatchStats.TryRemove(source);
			}
		}

		public void StopIndexing()
		{
			log.Debug("Stopping indexing workers");
			doIndexing = false;
			lock (waitForWork)
			{
				Monitor.PulseAll(waitForWork);
			}
		}

		public void StartIndexing()
		{
			doIndexing = true;
		}

		public void MarkAsRemovedFromIndex(HashSet<string> keys)
		{
			foreach (var key in keys)
			{
				recentlyDeleted.TryRemove(key);
			}
		}

		public bool ShouldRemoveFromIndex(string key)
		{
			var shouldRemoveFromIndex = recentlyDeleted.Contains(key);
			return shouldRemoveFromIndex;
		}

		public void MarkDeleted(string key)
		{
			recentlyDeleted.Add(key);
		}
	}
}
