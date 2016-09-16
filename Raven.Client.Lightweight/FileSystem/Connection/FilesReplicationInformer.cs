using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

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

        public Task UpdateReplicationInformationIfNeeded(IAsyncFilesCommands commands)
        {
            return UpdateReplicationInformationIfNeededInternal(commands);
        }

        private Task UpdateReplicationInformationIfNeededInternal(IAsyncFilesCommands commands)
        {
            if (Conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
                return new CompletedTask();

            if (LastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
                return new CompletedTask();

            var serverClient = (IAsyncFilesCommandsImpl)commands;
            lock (ReplicationLock)
            {
                if (FirstTime)
                {
                    var serverHash = ServerHash.GetServerHash(serverClient.UrlFor());
                    var document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
                    if (IsInvalidDestinationsDocument(document) == false)
                    {
                        UpdateReplicationInformationFromDocument(document);
                    }
                }

                FirstTime = false;

                if (LastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
                    return new CompletedTask();

                var taskCopy = RefreshReplicationInformationTask;
                if (taskCopy != null)
                    return taskCopy;

                return RefreshReplicationInformationTask = Task.Factory.StartNew(() => RefreshReplicationInformation(commands))
                    .ContinueWith(task =>
                    {
                        if (task.Exception != null)
                        {
                            log.ErrorException("Failed to refresh replication information", task.Exception);
                        }
                        RefreshReplicationInformationTask = null;
                    });
            }
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
                var urlForFilename = serverClient.UrlFor();
                var serverHash = ServerHash.GetServerHash(urlForFilename);
                JsonDocument document = null;

                try
                {
                    var destinations = serverClient.Synchronization.GetDestinationsAsync().Result;
                    failureCounts[urlForFilename] = new FailureCounter(); // we just hit the master, so we can reset its failure count

                    if (destinations != null)
                    {
                        document = new JsonDocument { DataAsJson = new RavenJObject() { { "Destinations", RavenJToken.FromObject(destinations) } } };
                    }
                }
                catch (Exception e)
                {
                    log.ErrorException("Could not contact master for new replication information", e);
                    document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
                }

                if (document == null)
                {
                    LastReplicationUpdate = SystemTime.UtcNow; // checked and not found
                    ReplicationDestinations = new List<OperationMetadata>(); // clear destinations that could be retrieved from local storage, but do not use clear because we might be in a middle of enumeration
                    return;
                }

                ReplicationInformerLocalCache.TrySavingReplicationInformationToLocalCache(serverHash, document);

                UpdateReplicationInformationFromDocument(document);

                LastReplicationUpdate = SystemTime.UtcNow;
            }
        }

        public override void ClearReplicationInformationLocalCache(IAsyncFilesCommands client)
        {
            var serverClient = (IAsyncFilesCommandsImpl)client;
            var urlForFilename = serverClient.UrlFor();
            var serverHash = ServerHash.GetServerHash(urlForFilename);
            ReplicationInformerLocalCache.ClearReplicationInformationFromLocalCache(serverHash);
        }

        protected override void UpdateReplicationInformationFromDocument(JsonDocument document)
        {
            var destinations = document.DataAsJson.Value<RavenJArray>("Destinations").Select(x => JsonConvert.DeserializeObject<SynchronizationDestination>(x.ToString()));
            ReplicationDestinations = destinations.Select(x =>
            {
                if (string.IsNullOrEmpty(x.Url) || x.Enabled == false)
                    return null;

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
    }
}
