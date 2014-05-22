using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;

namespace Raven.Database.Counters.Controllers
{
	public class RavenCounterReplication:IDisposable
	{
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly object waitForCounterUpdate = new object();
        private int actualWorkCounter = 0; // represents the number of changes in 
        private int replicatedWorkCounter = 0; // represents the last actualWorkCounter value that was checked in the last replication iteration
        private bool shouldPause = false;
        public bool IsRunning { get; private set; }
        private readonly ConcurrentDictionary<string, CounterDestinationStats> destinationsStats =
            new ConcurrentDictionary<string, CounterDestinationStats>(StringComparer.OrdinalIgnoreCase);
        private int replicationAttempts;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> activeReplicationTasks = new ConcurrentDictionary<string, SemaphoreSlim>();
        public readonly ConcurrentQueue<Task> activeTasks = new ConcurrentQueue<Task>();
        private HttpRavenRequestFactory httpRavenRequestFactory;

		public static string GetServerNameForWire(string server)
		{
			var uri = new Uri(server);
			return uri.Host + ":" + uri.Port;
		}

		private readonly CounterStorage storage;
		private readonly CancellationTokenSource cancellation;

		public RavenCounterReplication(CounterStorage storage)
		{
			this.storage = storage;
			this.storage.CounterUpdated += SignalCounterUpdate;
			cancellation = new CancellationTokenSource();
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
            var timeToWaitInMinutes = TimeSpan.FromMinutes(5);

            NotifySiblings();

            while (!cancellation.IsCancellationRequested)
            {
                SendReplicationToAllServers(runningBecauseOfDataModification);
                runningBecauseOfDataModification = WaitForCountersUpdate(timeToWaitInMinutes);
                timeToWaitInMinutes = runningBecauseOfDataModification ? TimeSpan.FromSeconds(30) : TimeSpan.FromMinutes(5);
            }

	        IsRunning = false;
	    }
        
	    public void SignalCounterUpdate()
	    {
	        lock (waitForCounterUpdate)
	        {
                Interlocked.Increment(ref actualWorkCounter);
	            Monitor.PulseAll(waitForCounterUpdate);
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

                Log.Debug("No counter updates for counter storage {0} was found, will wait for updates", storage.CounterStorageUrl);
                return Monitor.Wait(waitForCounterUpdate, timeout);
	        }
	    }
        public void Pause()
        {
            shouldPause = true;
        }

        public void Continue()
        {
            shouldPause = false;
        }


		private void RecordFailure(string url, string lastError)
		{
			var stats = destinationsStats.GetOrAdd(url, new CounterDestinationStats { Url = url });
			Interlocked.Increment(ref stats.FailureCountInternal);
			stats.LastFailureTimestamp = SystemTime.UtcNow;
			/*var stats = destinationStats.GetOrAdd(url, new DestinationStats { Url = url });
			Interlocked.Increment(ref stats.FailureCountInternal);
			stats.LastFailureTimestamp = SystemTime.UtcNow;
			if (string.IsNullOrWhiteSpace(lastError) == false)
				stats.LastError = lastError;
			*/
		}

        //Notifies servers which send us counters that we are back online
	    private void NotifySiblings() //TODO: implement
	    {
            /*try
        {
            collection.TryTake(out connectionStringOptions, 15 * 1000, docDb.WorkContext.CancellationToken);
            if (connectionStringOptions == null)
                return;
        }
        catch (Exception e)
        {
            log.ErrorException("Could not get connection string options to notify sibling servers about restart", e);
            return;
        }
        try
        {
            var url = connectionStringOptions.Url + "/replication/heartbeat?from=" + UrlEncodedServerUrl() + "&dbid=" + docDb.TransactionalStorage.Id;
            var request = httpRavenRequestFactory.Create(url, "POST", connectionStringOptions);
            request.WebRequest.ContentLength = 0;
            request.ExecuteRequest();
        }
        catch (Exception e)
        {
            log.WarnException("Could not notify " + connectionStringOptions.Url + " about sibling server being up & running", e);
        }*/
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
                        var url = connectionStringOptions.Url + "/counters/" + storage.CounterStorageName + "/replication/heartbeat?from=" + Uri.EscapeDataString(storage.CounterStorageUrl);
                        var request = httpRavenRequestFactory.Create(url, "POST", connectionStringOptions);
                        request.WebRequest.ContentLength = 0;
                        request.ExecuteRequest();
                    }
                    catch (Exception e)
                    {
                        Log.WarnException("Could not notify " + connectionStringOptions.Url + " about sibling server being up & running", e);
                    }
                }
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
			    Log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
			        destServerName, failureCount, shouldReplicateTo == false);
			    return shouldReplicateTo;
	        }
			return true;
        }

      

	    private ReplicationMessage GetCountersDataSinceEtag(long etag)
	    {
            var message = new ReplicationMessage { SendingServerName = storage.CounterStorageUrl };

            using (var reader = storage.CreateReader())
            {
                message.Counters = reader.GetCountersSinceEtag(etag + 1).Take(10240).ToList(); //TODO: Capped this...how to get remaining values?
            }

	        return message;
	    }

	    enum ReplicationResult
	    {
	        Success = 0,
            Failure = 1,
            NotReplicated = 2
	    }

		private bool ReplicateTo(CounterStorageReplicationDestination destination)
	    {
            //todo: here, build url according to :destination.Url + '/counters/' + destination.
	        try
	        {
	            string lastError;
	            bool result = false;

				switch (TryReplicate(destination, out lastError))
	            {
	                case ReplicationResult.Success:
						RecordSuccess(destination.CounterStorageUrl);
                        result = true;
	                    break;
                    case ReplicationResult.NotReplicated:
						//TODO: Record not replicated
                        break;
                    default:
                        RecordFailure(destination.CounterStorageUrl, lastError);
	                    break;
	            }

	            return result;
	        }
	        catch (Exception ex)
	        {
                Log.ErrorException("Error occured replicating to: " + destination.ServerUrl, ex);
                RecordFailure(destination.ServerUrl, ex.Message);
	            return false;
	        }
	        finally
	        {
                var holder = activeReplicationTasks.GetOrAdd(destination.ServerUrl, s => new SemaphoreSlim(0, 1));
                holder.Release();
	        }
	    }

		private RavenConnectionStringOptions GetConnectionOptionsSafe(CounterStorageReplicationDestination destination, out string lastError)
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

		private ReplicationResult TryReplicate(CounterStorageReplicationDestination destination, out string lastError)
        {
            long etag = 0;
            var connectionStringOptions = GetConnectionOptionsSafe(destination, out lastError);

			if (connectionStringOptions != null && GetLastReplicatedEtagFrom(connectionStringOptions, out etag, out lastError))
            {
                var replicationData = GetCountersDataSinceEtag(etag);

                if (replicationData.Counters.Count > 0)
                {
                    return PerformReplicationToServer(connectionStringOptions, etag, replicationData, out lastError) ?
                        ReplicationResult.Success : ReplicationResult.Failure;
                }

                return ReplicationResult.NotReplicated;
            }

            return ReplicationResult.Failure;
        }

	    private bool TryGetLastReplicatedEtagFrom(RavenConnectionStringOptions connectionStringOptions, out long lastEtag, out string lastError)
	    {
            lastEtag = 0;
            try
            {
                long etag = 0;
				var url = string.Format("{0}/lastEtag/{1}", connectionStringOptions.Url, GetServerNameForWire(storage.CounterStorageUrl));
                var request = httpRavenRequestFactory.Create(url, "GET", connectionStringOptions);
                request.ExecuteRequest(etagString => etag = long.Parse(etagString.ReadToEnd()));

                lastEtag = etag;
                lastError = string.Empty;
                return true;
            }
            catch (WebException e)
            {
				lastError = HandleReplicationDistributionWebException(e, connectionStringOptions.Url);
                return false;
            }
            catch (Exception e)
            {
                lastError = e.Message;
                return false;
            }
	    }
        
        private bool GetLastReplicatedEtagFrom(RavenConnectionStringOptions connectionStringOptions, out long lastEtag, out string lastError)
        {
	        if (!TryGetLastReplicatedEtagFrom(connectionStringOptions, out lastEtag, out lastError))
	        {
				if (IsFirstFailure(connectionStringOptions.Url))
	            {
	                return TryGetLastReplicatedEtagFrom(connectionStringOptions, out lastEtag, out lastError);
	            }
                return false;
	        }

	        return true;
	    }

		private bool TryPerformReplicationToServer(RavenConnectionStringOptions connectionStringOptions, ReplicationMessage message, out string lastError)
        {
            try
            {
                var url = string.Format("{0}/replication", connectionStringOptions.Url);
                lastError = string.Empty;
                var request = httpRavenRequestFactory.Create(url, "POST", connectionStringOptions);
                request.Write(message.GetRavenJObject());
                request.ExecuteRequest();
                return true;

            }
            catch (WebException e)
            {
                lastError = HandleReplicationDistributionWebException(e, connectionStringOptions.Url);
                return false;
            }
            catch (Exception e)
            {
                Log.ErrorException("Error occured replicating to: " + connectionStringOptions.Url, e);
                lastError = e.Message;
                return false;
            }
        }

		private bool PerformReplicationToServer(RavenConnectionStringOptions connectionStringOptions, long etag, ReplicationMessage message, out string lastError)
		{
			var destinationUrl = connectionStringOptions.Url;

			if (!TryPerformReplicationToServer(connectionStringOptions, message, out lastError))
	        {
	            if (IsFirstFailure(destinationUrl))
	            {
					return TryPerformReplicationToServer(connectionStringOptions, message, out lastError);
	            }
                return false;
	        }

	        return true;
	    }

        private bool IsFirstFailure(string destinationUrl)
        {
            var destStats = destinationsStats.GetOrAdd(destinationUrl, new CounterDestinationStats { Url = destinationUrl });
            return destStats.FailureCount == 0;
        }

		private void SendReplicationToAllServers(bool runningBecauseOfDataModifications)
		{
			IsRunning = !shouldPause;
			if (IsRunning)
			{
				try
				{
                    CounterStorageReplicationDocument replicationData;
					using (var reader = storage.CreateReader())
						replicationData = reader.GetReplicationData();

					if (replicationData != null && replicationData.Destinations.Count > 0)
				    {
				        var currentReplicationAttempts = Interlocked.Increment(ref replicationAttempts);

						var destinationForReplication = replicationData.Destinations.Where(
							destination => !runningBecauseOfDataModifications || IsNotFailing(destination.CounterStorageUrl, currentReplicationAttempts));
						
				        foreach (CounterStorageReplicationDestination destination in destinationForReplication)
				        {
							ReplicateToDestination(destination);
				        }
				    }
				}
				catch (Exception e)
				{
					Log.ErrorException("Failed to perform replication", e);
				}
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

		private void ReplicateToDestination(CounterStorageReplicationDestination destination)
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
                stats.LastReplicatedEtag = stats.LastEtagCheckedForReplication = lastReplicatedEtag.Value;
            }

            if (lastReplicatedLastModified.HasValue)
                stats.LastReplicatedLastModified = lastReplicatedLastModified;

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


    public class CounterDestinationStats
    {
        public int FailureCountInternal = 0;
        public string Url { get; set; }
        public DateTime? LastHeartbeatReceived { get; set; }
        public long LastEtagCheckedForReplication { get; set; }
        public long LastReplicatedEtag { get; set; }
        public DateTime? LastReplicatedLastModified { get; set; }
        public DateTime? LastSuccessTimestamp { get; set; }
        public DateTime? LastFailureTimestamp { get; set; }
        public int FailureCount { get { return FailureCountInternal; } }
        public string LastError { get; set; }
    }
}