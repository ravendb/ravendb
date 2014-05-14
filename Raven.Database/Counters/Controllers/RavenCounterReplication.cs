using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Server;

namespace Raven.Database.Counters.Controllers
{
	public class RavenCounterReplication
	{
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        private readonly object waitForCounterUpdate = new object();
        private int actualWorkCounter = 0; // represents the number of changes in 
        private int replicatedWorkCounter = 0; // represents the last actualWorkCounter value that was checked in the last replication iteration
        private bool shouldPause = false;
        public bool IsRunning { get; private set; }
		private readonly ConcurrentDictionary<string, DestinationStats> destinationsFailureCount = 
			new ConcurrentDictionary<string, DestinationStats>(StringComparer.OrdinalIgnoreCase);
        private int replicationAttempts;

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
			this.storage.CounterUpdated += workContext_CounterUpdated;
			cancellation = new CancellationTokenSource();
		}

		void workContext_CounterUpdated()
		{
			if (!cancellation.IsCancellationRequested)
				Replicate();
		}

		public void ShutDown()
		{
			cancellation.Cancel();
		}

	    public void StartReplication()
	    {
            var replicationTask = new Task(ReplicationAction, TaskCreationOptions.LongRunning);
            replicationTask.Start();
	    }

	    private void ReplicationAction()
	    {
	        var runningBecauseOfDataModification = false;
            var timeToWaitInMinutes = TimeSpan.FromMinutes(5);
            
            while (!cancellation.IsCancellationRequested)
            {
                Replicate(runningBecauseOfDataModification);
                //NotifySiblings(); TODO: implement
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

                log.Debug("No counter updates for counter storage {0} was found, will wait for updates", storage.Name);
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
			var stats = destinationsFailureCount.GetOrAdd(url, new DestinationStats { Url = url });
			Interlocked.Increment(ref stats.FailureCountInternal);
			stats.LastFailureTimestamp = SystemTime.UtcNow;
			/*var stats = destinationStats.GetOrAdd(url, new DestinationStats { Url = url });
			Interlocked.Increment(ref stats.FailureCountInternal);
			stats.LastFailureTimestamp = SystemTime.UtcNow;
			if (string.IsNullOrWhiteSpace(lastError) == false)
				stats.LastError = lastError;
			*/
		}
		 
		
		
		

		private bool IsNotFailing(string destServerName, int currentReplicationAttempts)
        {
	        DestinationStats destinationStats;
			if (destinationsFailureCount.TryGetValue(destServerName, out destinationStats))
			{
				bool shouldReplicateTo = false;
				var failureCount = destinationStats.FailureCount;
				if (failureCount > 1000)
				{
					shouldReplicateTo = currentReplicationAttempts % 10 == 0;
				}
				if (failureCount > 100)
				{
					shouldReplicateTo = currentReplicationAttempts % 5 == 0;
				}
				if (failureCount > 10)
				{
					shouldReplicateTo = currentReplicationAttempts % 2 == 0;
				}
				log.Debug("Failure count for {0} is {1}, skipping replication: {2}",
							destServerName, failureCount, shouldReplicateTo == false);
				return shouldReplicateTo;
	        }
			return true;
        }

		public void Replicate(bool runningBecauseOfDataModifications)
		{
			/*IsRunning = !shouldPause;
			if (IsRunning)
			{
				try
				{
				    var destinations = storage.Servers.Where(serverName => {serverName != storage.Name}).ToList();

				    if (destinations.Count > 0)
				    {
				        var currentReplicationAttempts = Interlocked.Increment(ref replicationAttempts);

				        var destinationForReplication = destinations.Where(
				            serverName => !runningBecauseOfDataModifications || IsNotFailing(serverName, currentReplicationAttempts));

				        var startedTasks = new List<Task>();

				        foreach (var destinationUrl in destinationForReplication)
				        {
				            var dest = destinationUrl;
				            var holder = activeReplicationTasks.GetOrAdd(dest, new IntHolder());
				            if (Thread.VolatileRead(ref holder.Value) == 1) continue;
				            Thread.VolatileWrite(ref holder.Value, 1);
				            var replicationTask = Task.Factory.StartNew(
				                () =>
				                {
				                    using (LogContext.WithDatabase(docDb.Name))
				                    {
				                        try
				                        {
				                            if (ReplicateTo(dest)) docDb.WorkContext.NotifyAboutWork();
				                        }
				                        catch (Exception e)
				                        {
				                            log.ErrorException("Could not replicate to " + dest, e);
				                        }
				                    }
				                });

				            startedTasks.Add(replicationTask);

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

				        Task.WhenAll(startedTasks.ToArray()).ContinueWith(
				            t =>
				            {
				                if (destinationStats.Count != 0)
				                {
				                    var minLastReplicatedEtag =
				                        destinationStats.Where(x => x.Value.LastReplicatedEtag != null)
				                                        .Select(x => x.Value.LastReplicatedEtag)
				                                        .Min(x => new ComparableByteArray(x.ToByteArray()));

				                    if (minLastReplicatedEtag != null) prefetchingBehavior.CleanupDocuments(minLastReplicatedEtag.ToEtag());
				                }
				            }).AssertNotFailed();
				    }
				}
				catch (Exception e)
				{
				    log.ErrorException("Failed to perform replication", e);
				}
			}*/




			    /*var tasks =
				    storage.Servers
					    .Where(x => x != storage.Name) //skip "this" server
					    .Select(async server =>
					    {
						    var http = new HttpClient();
						    var etagResult = await http.GetStringAsync(string.Format("{0}/lastEtag/{1}", server, GetServerNameForWire(storage.Name)));
						    var etag = int.Parse(etagResult);
						    var message = new ReplicationMessage {SendingServerName = storage.Name};
						    using (var reader = storage.CreateReader())
						    {
							    message.Counters = reader.GetCountersSinceEtag(etag).Take(1024).ToList(); //TODO: Capped this...how to get remaining values?
						    }
						    var url = string.Format("{0}/replication", server);
						    return message.Counters.Count > 0 ?
							    new HttpClient().PostAsync(url, message, new JsonMediaTypeFormatter()) :
							    Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotModified)); //HACK: could do something else here
					    });

			    try
			    {
				    await Task.WhenAll(tasks);
			    }
			    catch (Exception)
			    {
                
				    //TODO: log
			    }*/
		}
	}
}