using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Connection
{
    /// <summary>
    /// Replication and failover management on the client side
    /// </summary>
    public class FilesReplicationInformer : ReplicationInformerBase<IAsyncFilesCommands>, IFilesReplicationInformer
    {
        public FilesReplicationInformer(Convention conventions, HttpJsonRequestFactory requestFactory)
            : base(conventions, requestFactory)
        {
        }


        /// <summary>
        /// Refreshes the replication information.
        /// Expert use only.
        /// </summary>
        public override void RefreshReplicationInformation(IAsyncFilesCommands commands)
        {            
            lock (this)
            {
                var serverClient = (IAsyncFilesCommandsImpl)commands;

                string urlForFilename = serverClient.UrlFor();
                var serverHash = ServerHash.GetServerHash(urlForFilename);

                JsonDocument document = null;

                try
                {
                    var config = serverClient.Configuration.GetKeyAsync<RavenJObject>(SynchronizationConstants.RavenSynchronizationDestinations).Result;
                    failureCounts[urlForFilename] = new FailureCounter(); // we just hit the master, so we can reset its failure count

                    if (config != null)
                    {

                        var destinationsArray = config.Value<RavenJArray>("Destinations");
                        if (destinationsArray != null)
                        {
                            document = new JsonDocument();
                            document.DataAsJson = new RavenJObject() { { "Destinations", destinationsArray } };
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
            var destinations = document.DataAsJson.Value<RavenJArray>("Destinations").Select(x => JsonConvert.DeserializeObject<SynchronizationDestination>(x.ToString()));
            ReplicationDestinations = destinations.Select(x =>
            {
                ICredentials credentials = null;
                if (string.IsNullOrEmpty(x.Username) == false)
                {
                    credentials = string.IsNullOrEmpty(x.Domain)
                                      ? new NetworkCredential(x.Username, x.Password)
                                      : new NetworkCredential(x.Username, x.Password, x.Domain);
                }

                return new OperationMetadata(x.Url, new OperationCredentials(x.ApiKey, credentials));
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

        protected override string GetServerCheckUrl(string baseUrl)
        {
            return baseUrl + "/config?name=" + Uri.EscapeDataString(SynchronizationConstants.RavenSynchronizationDestinations) + "&check-server-reachable";
        }

        public override Task UpdateReplicationInformationIfNeeded(IAsyncFilesCommands commands)
        {
            var serverClient = (IAsyncFilesCommandsImpl)commands;

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
