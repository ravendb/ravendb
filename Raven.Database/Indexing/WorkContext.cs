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
using Raven.Database.Storage;
using System.Linq;
using Raven.Database.Util;

namespace Raven.Database.Indexing
{
	public class WorkContext : IDisposable
	{
		private readonly ConcurrentSet<FutureBatchStats> futureBatchStats = new ConcurrentSet<FutureBatchStats>();

		private readonly SizeLimitedConcurrentSet<string> recentlyDeleted = new SizeLimitedConcurrentSet<string>(100, StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentQueue<ActualIndexingBatchSize> lastActualIndexingBatchSize = new ConcurrentQueue<ActualIndexingBatchSize>();
		private readonly ConcurrentQueue<ServerError> serverErrors = new ConcurrentQueue<ServerError>();
		private readonly object waitForWork = new object();
		private volatile bool doWork = true;
		private volatile bool doIndexing = true;
		private int workCounter;
		private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
		private static readonly ILog log = LogManager.GetCurrentClassLogger();
		private readonly ThreadLocal<List<Func<string>>> shouldNotifyOnWork = new ThreadLocal<List<Func<string>>>(() => new List<Func<string>>());
		public OrderedPartCollection<AbstractIndexUpdateTrigger> IndexUpdateTriggers { get; set; }
		public OrderedPartCollection<AbstractReadTrigger> ReadTriggers { get; set; }
		public string DatabaseName { get; set; }
		public Dictionary<string,string> CountersNames = new Dictionary<string, string>();

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

		public InMemoryRavenConfiguration Configuration { get; set; }
		public IndexStorage IndexStorage { get; set; }

		public TaskScheduler TaskScheduler { get; set; }
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
		private PerformanceCounter RequestsPerSecCounter { get; set; }
		private PerformanceCounter ConcurrentRequestsCounter { get; set; }
		private bool useCounters = true;

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
				if(useCounters == false)
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
		private void CreatePreformanceCounters(string name)
		{
			var categoryName = "RavenDB 2.0: " + name;
			DocsPerSecCounter = new PerformanceCounter
			{
				CategoryName = categoryName,
				CounterName = "# docs / sec",
				MachineName = ".",
				ReadOnly = false
			};

			IndexedPerSecCounter = new PerformanceCounter
			{
				CategoryName = categoryName,
				CounterName = "# docs indexed / sec",
				MachineName = ".",
				ReadOnly = false
			};

			ReducedPerSecCounter = new PerformanceCounter
			{
				CategoryName = categoryName,
				CounterName = "# docs reduced / sec",
				MachineName = ".",
				ReadOnly = false
			};

			RequestsPerSecCounter = new PerformanceCounter
			{
				CategoryName = categoryName,
				CounterName = "# req / sec",
				MachineName = ".",
				ReadOnly = false
			};

			ConcurrentRequestsCounter = new PerformanceCounter
			{
				CategoryName = categoryName,
				CounterName = "# of concurrent requests",
				MachineName = ".",
				ReadOnly = false
			};

		}

		private void SetupPreformanceCounter(string name)
		{
			if (PerformanceCounterCategory.Exists("RavenDB 2.0: " + name))
				return;

			var counters = new CounterCreationDataCollection();

			var docsPerSecond = new CounterCreationData
			{
				CounterName = "# docs / sec",
				CounterHelp = "Number of documents added per second",
				CounterType = PerformanceCounterType.RateOfCountsPerSecond32
			};
			counters.Add(docsPerSecond);

			var indexedPerSecond = new CounterCreationData
			{
				CounterName = "# docs indexed / sec",
				CounterHelp = "Number of documents indexed per second",
				CounterType = PerformanceCounterType.RateOfCountsPerSecond32
			};
			counters.Add(indexedPerSecond);

			var reducesPerSecond = new CounterCreationData
			{
				CounterName = "# docs reduced / sec",
				CounterHelp = "Number of documents reduced per second",
				CounterType = PerformanceCounterType.RateOfCountsPerSecond32
			};
			counters.Add(reducesPerSecond);

			var requestsPerSecond = new CounterCreationData
			{
				CounterName = "# req / sec",
				CounterHelp = "Number of http requests per second",
				CounterType = PerformanceCounterType.RateOfCountsPerSecond32
			};
			counters.Add(requestsPerSecond);

			var concurrentRequests = new CounterCreationData
			{
				CounterName = "# of concurrent requests",
				CounterHelp = "Number of http requests per second",
				CounterType = PerformanceCounterType.NumberOfItems32
			};
			counters.Add(concurrentRequests);

			// create new category with the counters above

			PerformanceCounterCategory.Create("RavenDB 2.0: " + name,
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
				SetupPreformanceCounterName(name);
				SetupPreformanceCounter(CountersNames[name]);
				CreatePreformanceCounters(CountersNames[name]);
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

		private void SetupPreformanceCounterName(string name)
		{
			//For databases that has a long name
			if (CountersNames.ContainsKey(name))
				return;

			string result = name;
			//dealing with names who are very long (there is a limit of 80 chars for counter name)
			if (result.Length > 60)
			{
				result = name.Remove(59);
				int counter = 1;
				while (PerformanceCounterCategory.Exists("RavenDB 2.0: " + result + counter))
				{
					counter++;
				}
				result = result + counter;
			}

			CountersNames.Add(name,result);
		}

		public void ReportIndexingActualBatchSize(int size)
		{
			lastActualIndexingBatchSize.Enqueue(new ActualIndexingBatchSize
			{
				Size = size,
				Timestamp = SystemTime.UtcNow
			});
			if (lastActualIndexingBatchSize.Count > 25)
			{
				ActualIndexingBatchSize tuple;
				lastActualIndexingBatchSize.TryDequeue(out tuple);
			}
		}

		public ConcurrentSet<FutureBatchStats> FutureBatchStats
		{
			get { return futureBatchStats; }
		}

		public ConcurrentQueue<ActualIndexingBatchSize> LastActualIndexingBatchSize
		{
			get { return lastActualIndexingBatchSize; }
		}

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