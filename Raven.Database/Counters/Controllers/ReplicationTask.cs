using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Controllers
{
	public class ReplicationTask: IDisposable
	{
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly object waitForCounterUpdate = new object();
        private int actualWorkCounter;
        private int replicatedWorkCounter; // represents the last actualWorkCounter value that was checked in the last replication iteration

		private readonly ConcurrentDictionary<string, CounterDestinationStats> destinationsStats =
            new ConcurrentDictionary<string, CounterDestinationStats>(StringComparer.OrdinalIgnoreCase);
        private int replicationAttempts;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> activeReplicationTasks = new ConcurrentDictionary<string, SemaphoreSlim>();
        private readonly ConcurrentQueue<Task> activeTasks = new ConcurrentQueue<Task>();
        private HttpRavenRequestFactory httpRavenRequestFactory;

		private readonly CounterStorage storage;
		private readonly CancellationTokenSource cancellation;

		enum ReplicationResult
		{
			Success = 0,
			Failure = 1,
			NotReplicated = 2
		}

		public ReplicationTask(CounterStorage storage)
		{			
			this.storage = storage;
			this.storage.CounterUpdated += SignalCounterUpdate;
			cancellation = new CancellationTokenSource();
		}

		public void SignalCounterUpdate()
		{
			lock (waitForCounterUpdate)
			{
				Interlocked.Increment(ref actualWorkCounter);
				Monitor.PulseAll(waitForCounterUpdate);
			}
		}

	    public void StartReplication()
	    {
            var replicationTask = new Task(ReplicationAction, TaskCreationOptions.LongRunning);

            httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = storage.ReplicationTimeoutInMs };
            replicationTask.Start();
	    }

		private void ReplicationAction()
	    {
	        var runningBecauseOfDataModification = false;
			var timeToWait = TimeSpan.FromMilliseconds(storage.Configuration.Counter.ReplicationLatencyInMs * 10);

            //NotifySiblings(); //TODO: implement

            while (!cancellation.IsCancellationRequested)
            {
                SendReplicationToAllServers(runningBecauseOfDataModification);
                runningBecauseOfDataModification = WaitForCountersUpdate(timeToWait);
				timeToWait = runningBecauseOfDataModification ? 
					TimeSpan.FromMilliseconds(storage.Configuration.Counter.ReplicationLatencyInMs) : //by default this is 30 sec
					TimeSpan.FromMilliseconds(storage.Configuration.Counter.ReplicationLatencyInMs * 10); //by default this is 5 min
            }
	    }

		private bool WaitForCountersUpdate(TimeSpan timeout)
		{
			if (Thread.VolatileRead(ref actualWorkCounter) != replicatedWorkCounter)
			{
				replicatedWorkCounter = actualWorkCounter;
				return true;
			}
			lock (waitForCounterUpdate)
			{
				if (Thread.VolatileRead(ref actualWorkCounter) != replicatedWorkCounter)
				{
					replicatedWorkCounter = actualWorkCounter;
					return true;
				}

				if (Log.IsDebugEnabled)
					Log.Debug("No counter updates for counter storage {0} was found, will wait for updates", storage.CounterStorageUrl);
				return Monitor.Wait(waitForCounterUpdate, timeout);
			}
		}

		public void HandleHeartbeat(string src)
		{
			ResetFailureForHeartbeat(src);
		}

		private void ResetFailureForHeartbeat(string src)
		{
			RecordSuccess(src, lastHeartbeatReceived: SystemTime.UtcNow);
			SignalCounterUpdate();
		}

		public void SendReplicationToAllServers(bool runningBecauseOfDataModifications)
		{
			try
			{
				var replicationDestinations = GetReplicationDestinations();
				if (replicationDestinations == null || replicationDestinations.Count <= 0) 
					return;

				var currentReplicationAttempts = Interlocked.Increment(ref replicationAttempts);

				var destinationForReplication = replicationDestinations.Where(destination => 
					(!runningBecauseOfDataModifications || IsNotFailing(destination.CounterStorageUrl, currentReplicationAttempts)) && !destination.Disabled);

				foreach (var destination in destinationForReplication)
				{
					ReplicateToDestination(destination);
				}
			}
			catch (Exception e)
			{
				Log.ErrorException("Failed to perform replication", e);
			}
		}

		private void ReplicateToDestination(CounterReplicationDestination destination)
		{
			var dest = destination.CounterStorageUrl;
			var holder = activeReplicationTasks.GetOrAdd(dest, s => new SemaphoreSlim(1));
			if (holder.Wait(0) == false)
				return;
			var replicationTask = Task.Factory.StartNew(
				() =>
				{
					//using (LogContext.WithDatabase(storage.Name)) //TODO: log with counter storage contexe
					//{
					try
					{
						if (ReplicateTo(destination)) SignalCounterUpdate();
					}
					catch (Exception e)
					{
						Log.ErrorException("Could not replicate to " + dest, e);
					}
					//}
				});

			activeTasks.Enqueue(replicationTask);
			replicationTask.ContinueWith(
				_ =>
				{
					// here we purge all the completed tasks at the head of the queue
					Task task;
					while (activeTasks.TryPeek(out task))
					{
						if (!task.IsCompleted && !task.IsCanceled && !task.IsFaulted) break;
						activeTasks.TryDequeue(out task); // remove it from end
					}
				});
		}

		private bool ReplicateTo(CounterReplicationDestination destination)
		{
            var replicationStopwatch = Stopwatch.StartNew();
			//todo: here, build url according to :destination.Url + '/counters/' + destination.
			try
			{
				string lastError;
			    long lastEtag;
				bool result = false;

				switch (TryReplicate(destination, out lastEtag, out lastError))
				{
					case ReplicationResult.Success:
                        DateTime replicationTime = SystemTime.UtcNow;
                        RecordSuccess(destination.CounterStorageUrl, lastReplicatedEtag: lastEtag, lastReplicatedLastModified: replicationTime);
                        storage.MetricsCounters.OutgoingReplications.Mark();
						result = true;
						break;
					case ReplicationResult.NotReplicated:
						//TODO: Record not replicated
                        RecordSuccess(destination.CounterStorageUrl, SystemTime.UtcNow);
						break;
					default:
						RecordFailure(destination.CounterStorageUrl, lastError);
                        storage.MetricsCounters.OutgoingReplications.Mark();
						break;
				}

				return result;
			}
			catch (Exception ex)
			{
				Log.ErrorException("Error occured replicating to: " + destination.CounterStorageUrl, ex);
				RecordFailure(destination.CounterStorageUrl, ex.Message);
				return false;
			}
			finally
			{
                replicationStopwatch.Stop();
			    var elapsedMicroseconds = (long) (replicationStopwatch.Elapsed.TotalMilliseconds*SystemTime.MicroSecPerTick);
                storage.MetricsCounters.GetReplicationDurationHistogram(destination.CounterStorageUrl).Update(elapsedMicroseconds);
				var holder = activeReplicationTasks.GetOrAdd(destination.CounterStorageUrl, s => new SemaphoreSlim(0, 1));
				holder.Release();
			}
		}

		private ReplicationResult TryReplicate(CounterReplicationDestination destination, out long lastEtagSent, out string lastError)
		{
            long etag;
		    lastEtagSent = 0;
			var connectionStringOptions = GetConnectionOptionsSafe(destination, out lastError);

			if (connectionStringOptions == null ||
				!GetLastReplicatedEtagFrom(connectionStringOptions, destination.CounterStorageUrl, out etag, out lastError)) 
				return ReplicationResult.Failure;

			var replicationData = GetCountersDataSinceEtag(etag, out lastEtagSent);
                
			storage.MetricsCounters.GetReplicationBatchSizeMetric(destination.CounterStorageUrl).Mark(replicationData.Counters.Count);
			storage.MetricsCounters.GetReplicationBatchSizeHistogram(destination.CounterStorageUrl).Update(replicationData.Counters.Count);

			if (replicationData.Counters.Count > 0)
			{
				return PerformReplicationToServer(connectionStringOptions, destination.CounterStorageUrl, replicationData, out lastError) ?
					ReplicationResult.Success : ReplicationResult.Failure;
			}

			return ReplicationResult.NotReplicated;
		}

		private bool GetLastReplicatedEtagFrom(RavenConnectionStringOptions connectionStringOptions, string counterStorageUrl, out long lastEtag, out string lastError)
		{
			if (!TryGetLastReplicatedEtagFrom(connectionStringOptions, counterStorageUrl, out lastEtag, out lastError))
			{
				if (IsFirstFailure(connectionStringOptions.Url))
				{
					return TryGetLastReplicatedEtagFrom(connectionStringOptions, counterStorageUrl, out lastEtag, out lastError);
				}
				return false;
			}

			return true;
		}

		private bool TryGetLastReplicatedEtagFrom(RavenConnectionStringOptions connectionStringOptions, string counterStorageUrl, out long lastEtag, out string lastError)
		{
			lastEtag = 0;
			try
			{
				long etag = 0;
				var url = string.Format("{0}/lastEtag?serverId={1}", counterStorageUrl, storage.ServerId);
				var request = httpRavenRequestFactory.Create(url, HttpMethods.Get, connectionStringOptions);
				
				request.ExecuteRequest(etagString => etag = long.Parse(etagString.ReadToEnd()));
				
				lastEtag = etag;
				lastError = string.Empty;
				return true;
			}
			catch (WebException e)
			{
				lastError = HandleReplicationDistributionWebException(e, counterStorageUrl);
				return false;
			}
			catch (Exception e)
			{
				lastError = e.Message;
				return false;
			}
		}

		private bool PerformReplicationToServer(RavenConnectionStringOptions connectionStringOptions, string counterStorageUrl, ReplicationMessage message, out string lastError)
		{
			var destinationUrl = connectionStringOptions.Url;

			if (!TryPerformReplicationToServer(connectionStringOptions, counterStorageUrl, message, out lastError))
			{
				if (IsFirstFailure(destinationUrl))
				{
					return TryPerformReplicationToServer(connectionStringOptions, counterStorageUrl, message, out lastError);
				}
				return false;
			}

			return true;
		}

		private bool TryPerformReplicationToServer(RavenConnectionStringOptions connectionStringOptions, string counterStorageUrl, ReplicationMessage message, out string lastError)
		{
			try
			{
				var url = string.Format("{0}/replication", counterStorageUrl);
				lastError = string.Empty;
				var request = httpRavenRequestFactory.Create(url, HttpMethods.Post, connectionStringOptions);
				request.Write(RavenJObject.FromObject(message));
				request.ExecuteRequest();
                
                return true;
			}
			catch (WebException e)
			{
				lastError = HandleReplicationDistributionWebException(e, counterStorageUrl);
				return false;
			}
			catch (Exception e)
			{
				Log.ErrorException("Error occured replicating to: " + counterStorageUrl, e);
				lastError = e.Message;
				return false;
			}
		}

		private List<CounterReplicationDestination> GetReplicationDestinations()
		{
			CountersReplicationDocument replicationData;
			using (var reader = storage.CreateReader())
				replicationData = reader.GetReplicationData();
			return (replicationData != null) ? replicationData.Destinations : null;
		}
		
		private bool IsNotFailing(string destServerName, int currentReplicationAttempts)
        {
            CounterDestinationStats destinationStats;
            if (destinationsStats.TryGetValue(destServerName, out destinationStats) && destinationStats.FailureCount > 10)
			{
				bool shouldReplicateTo = false;
				var failureCount = destinationStats.FailureCount;

			    if (failureCount > 1000)
			    {
			        shouldReplicateTo = currentReplicationAttempts%10 == 0;
			    }
			    if (failureCount > 100)
			    {
			        shouldReplicateTo = currentReplicationAttempts%5 == 0;
			    }
			    if (failureCount > 10)
			    {
			        shouldReplicateTo = currentReplicationAttempts%2 == 0;
			    }

				if (Log.IsDebugEnabled)
					Log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
			        destServerName, failureCount, shouldReplicateTo == false);
			    return shouldReplicateTo;
	        }
			return true;
        }

	    private ReplicationMessage GetCountersDataSinceEtag(long etag, out long lastEtagSent)
	    {
			var message = new ReplicationMessage { ServerId = storage.ServerId, SendingServerName = storage.CounterStorageUrl };

            using (var reader = storage.CreateReader())
            {
                message.Counters = reader.GetCountersSinceEtag(etag + 1).Take(1024).ToList(); //TODO: Capped this...how to get remaining values?
                lastEtagSent = message.Counters.Count > 0 ? message.Counters.Max(x => x.Etag) : etag; // change this once changed this function do a reall paging
            }

	        return message;
	    }

		private RavenConnectionStringOptions GetConnectionOptionsSafe(CounterReplicationDestination destination, out string lastError)
		{
			try
			{
				var connectionStringOptions = new RavenConnectionStringOptions
				{
                    Url = destination.ServerUrl,
					ApiKey = destination.ApiKey,
				};
				if (string.IsNullOrEmpty(destination.Username) == false)
				{
					connectionStringOptions.Credentials = string.IsNullOrEmpty(destination.Domain)
						? new NetworkCredential(destination.Username, destination.Password)
						: new NetworkCredential(destination.Username, destination.Password, destination.Domain);
				}
				lastError = string.Empty;
				return connectionStringOptions;
			}
			catch (Exception e)
			{
				lastError = e.Message;
				Log.ErrorException(string.Format("Ignoring bad replication config!{0}Could not figure out connection options for [Url: {1}]",
                    Environment.NewLine, destination.ServerUrl), e);
				return null;
			}
		}

        private bool IsFirstFailure(string destinationUrl)
        {
            var destStats = destinationsStats.GetOrAdd(destinationUrl, new CounterDestinationStats { Url = destinationUrl });
            return destStats.FailureCount == 0;
        }

		//Notifies servers which send us counters that we are back online
		private void NotifySiblings() //TODO: implement
		{
			var notifications = new BlockingCollection<RavenConnectionStringOptions>();

			Task.Factory.StartNew(() => NotifySibling(notifications));

			var replicationDestinations = GetReplicationDestinations();
			foreach (var replicationDestination in replicationDestinations)
			{
				string lastError;
				notifications.TryAdd(GetConnectionOptionsSafe(replicationDestination, out lastError), 15 * 1000);
			}

			//TODO: add to notifications to the source server, the servers we get the replications from
		}

		private void NotifySibling(BlockingCollection<RavenConnectionStringOptions> collection)
		{
			// using (LogContext.WithDatabase(docDb.Name)) todo:implement log context
			while (true)
			{
				RavenConnectionStringOptions connectionStringOptions;
				try
				{
					collection.TryTake(out connectionStringOptions, 15 * 1000, cancellation.Token);
					if (connectionStringOptions == null)
						return;
				}
				catch (Exception e)
				{
					Log.ErrorException("Could not get connection string options to notify sibling servers about restart", e);
					return;
				}
				try
				{
					var url = connectionStringOptions.Url + "/cs/" + storage.Name + "/replication/heartbeat?from=" + Uri.EscapeDataString(storage.CounterStorageUrl);
					var request = httpRavenRequestFactory.Create(url, HttpMethods.Post, connectionStringOptions);
					request.WebRequest.ContentLength = 0;
					request.ExecuteRequest();
				}
				catch (Exception e)
				{
					Log.WarnException("Could not notify " + connectionStringOptions.Url + " about sibling server being up & running", e);
				}
			}
		}
        
        private void RecordSuccess(string url,
            DateTime? lastSuccessTimestamp = null, 
            long? lastReplicatedEtag = null,
            DateTime? lastReplicatedLastModified = null,
            DateTime? lastHeartbeatReceived = null, string lastError = null)
        {
            var stats = destinationsStats.GetOrAdd(url, new CounterDestinationStats { Url = url });
            Interlocked.Exchange(ref stats.FailureCountInternal, 0);

            if (lastSuccessTimestamp.HasValue)
            {
                stats.LastSuccessTimestamp = lastSuccessTimestamp.Value;
            }

            if (lastReplicatedEtag.HasValue)
            {
                stats.LastReplicatedEtag = lastReplicatedEtag.Value;
            }

            if (lastReplicatedLastModified.HasValue)
                stats.LastSuccessTimestamp = stats.LastReplicatedLastModified = lastReplicatedLastModified;

            if (lastHeartbeatReceived.HasValue)
            {
                stats.LastHeartbeatReceived = lastHeartbeatReceived;
            }
            else
            {
                stats.LastHeartbeatReceived = SystemTime.UtcNow;
            }

            if (!string.IsNullOrWhiteSpace(lastError))
                stats.LastError = lastError;
        }

		private void RecordFailure(string url, string lastError)
		{
			var stats = destinationsStats.GetOrAdd(url, new CounterDestinationStats { Url = url });
			Interlocked.Increment(ref stats.FailureCountInternal);
			stats.LastFailureTimestamp = SystemTime.UtcNow;
			if (string.IsNullOrWhiteSpace(lastError) == false)
			{
				stats.LastError = lastError;
			}
		}

		private string HandleReplicationDistributionWebException(WebException e, string destinationUrl)
		{
			var response = e.Response as HttpWebResponse;
			if (response != null)
			{
				Stream responseStream = response.GetResponseStream();
				if (responseStream != null)
				{
					using (var streamReader = new StreamReader(responseStream))
					{
						var error = streamReader.ReadToEnd();
						Log.WarnException("Replication to " + destinationUrl + " had failed\r\n" + error, e);
					}
				}
				else
				{
					Log.WarnException("Replication to " + destinationUrl + " had failed", e);
				}
			}
			else
			{
				Log.WarnException("Replication to " + destinationUrl + " had failed", e);
			}

			return e.Message;
		}

	    public int GetActiveTasksCount()
	    {
	        return activeTasks.Count;
	    }

        public ConcurrentDictionary<string, CounterDestinationStats> DestinationStats
        {
            get { return destinationsStats; }
        }

		public void Dispose()
        {
            Task task;
            cancellation.Cancel();
            SignalCounterUpdate();

            while (activeTasks.TryDequeue(out task))
            {
                task.Wait();
            }
        }
    }
    
}