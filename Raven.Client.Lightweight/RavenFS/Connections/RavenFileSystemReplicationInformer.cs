using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.RavenFS.Connections
{
    /// <summary>
    /// Replication and failover management on the client side
    /// </summary>
    public class RavenFileSystemReplicationInformer : ReplicationInformerBase<RavenFileSystemClient>, IFileSystemClientReplicationInformer
    {
        public RavenFileSystemReplicationInformer(Convention conventions) : base(conventions)
        {
        }



        /// <summary>
        /// Refreshes the replication information.
        /// Expert use only.
        /// </summary>
        public override void RefreshReplicationInformation(RavenFileSystemClient serverClient)
        {
            lock (this)
            {
                var serverHash = ServerHash.GetServerHash(serverClient.FileSystemUrl);

                JsonDocument document = null;

                try
                {
                    var config = serverClient.Config.GetConfig(SynchronizationConstants.RavenSynchronizationDestinations).Result;
                    failureCounts[serverClient.FileSystemUrl] = new FailureCounter(); // we just hit the master, so we can reset its failure count

                    if (config != null)
                    {
                        var destinationStrings = config.GetValues("destination");

                        if (destinationStrings != null)
                        {
                            var destinations = destinationStrings.Select(JsonConvert.DeserializeObject<SynchronizationDestination>).ToList();

                            var ravenJArray = RavenJToken.FromObject(destinations);

                            document = new JsonDocument();
                            document.DataAsJson = new RavenJObject(){{"destinations", ravenJArray}};
                        }
                    }
                }
                catch (Exception e)
                {
                    log.ErrorException("Could not contact master for new replication information", e);
                    document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
                }


                if (document == null)
                {
                    lastReplicationUpdate = SystemTime.UtcNow; // checked and not found
                    return;
                }

                ReplicationInformerLocalCache.TrySavingReplicationInformationToLocalCache(serverHash, document);

                UpdateReplicationInformationFromDocument(document);

                lastReplicationUpdate = SystemTime.UtcNow;
            }
        }

        protected override void UpdateReplicationInformationFromDocument(JsonDocument document)
        {
            var destinations = document.DataAsJson.Value<RavenJArray>("destinations").Select(x => JsonConvert.DeserializeObject<SynchronizationDestination>(x.ToString()));
            ReplicationDestinations = destinations.Select(x =>
            {
                ICredentials credentials = null;
                if (string.IsNullOrEmpty(x.Username) == false)
                {
                    credentials = string.IsNullOrEmpty(x.Domain)
                                      ? new NetworkCredential(x.Username, x.Password)
                                      : new NetworkCredential(x.Username, x.Password, x.Domain);
                }

                return new OperationMetadata(x.FileSystemUrl, new OperationCredentials(x.ApiKey, credentials));
            })
                // filter out replication destination that don't have the url setup, we don't know how to reach them
                // so we might as well ignore them. Probably private replication destination (using connection string names only)
                .Where(x => x != null)
                .ToList();
            foreach (var replicationDestination in ReplicationDestinations)
            {
                FailureCounter value;
                if (failureCounts.TryGetValue(replicationDestination.Url, out value))
                    continue;
                failureCounts[replicationDestination.Url] = new FailureCounter();
            }

        }

        public override Task UpdateReplicationInformationIfNeeded(RavenFileSystemClient serverClient)
		{
			if (conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
				return new CompletedTask();

			if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
				return new CompletedTask();

			lock (replicationLock)
			{
				if (firstTime)
				{
					var serverHash = ServerHash.GetServerHash(serverClient.ServerUrl);

					var destinations = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
					if (destinations != null)
					{
						UpdateReplicationInformationFromDocument(destinations);
					}
				}

				firstTime = false;

				if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
					return new CompletedTask();

				var taskCopy = refreshReplicationInformationTask;
				if (taskCopy != null)
					return taskCopy;

                return refreshReplicationInformationTask = Task.Factory.StartNew(() => RefreshReplicationInformation(serverClient))
					.ContinueWith(task =>
					{
						if (task.Exception != null)
						{
							log.ErrorException("Failed to refresh replication information", task.Exception);
						}
						refreshReplicationInformationTask = null;
					});
			}
		}
    }
}
	