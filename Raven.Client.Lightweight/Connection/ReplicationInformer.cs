// -----------------------------------------------------------------------
//  <copyright file="ReplicationInformation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
    public class ReplicationInformer : ReplicationInformerBase<ServerClient>, IDocumentStoreReplicationInformer
    {
        public ReplicationInformer(Convention conventions, HttpJsonRequestFactory jsonRequestFactory)
            : base(conventions, jsonRequestFactory)
        {
        }

        /// <summary>
        /// Failover servers set manually in config file or when document store was initialized
        /// </summary>
        public ReplicationDestination[] FailoverServers { get; set; }

        public Task UpdateReplicationInformationIfNeeded(AsyncServerClient serverClient)
        {
            return UpdateReplicationInformationIfNeededInternal(serverClient.Url, () => AsyncHelpers.RunSync(() => serverClient.DirectGetReplicationDestinationsAsync(new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials))));
        }

        private Task UpdateReplicationInformationIfNeededInternal(string url, Func<ReplicationDocument> getReplicationDestinations)
        {
            if (Conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
                return new CompletedTask();

            if (LastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
                return new CompletedTask();

            lock (ReplicationLock)
            {
                if (FirstTime)
                {
                    var serverHash = ServerHash.GetServerHash(url);

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

                return RefreshReplicationInformationTask = Task.Factory.StartNew(() => RefreshReplicationInformationInternal(url, getReplicationDestinations))
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

        public override void ClearReplicationInformationLocalCache(ServerClient client)
        {
            var serverHash = ServerHash.GetServerHash(client.Url);
            ReplicationInformerLocalCache.ClearReplicationInformationFromLocalCache(serverHash);
        }

        protected override void UpdateReplicationInformationFromDocument(JsonDocument document)
        {
            var replicationDocument = document.DataAsJson.JsonDeserialization<ReplicationDocument>();
            ReplicationDestinations = replicationDocument.Destinations.Select(x =>
            {
                var url = string.IsNullOrEmpty(x.ClientVisibleUrl) ? x.Url : x.ClientVisibleUrl;
                if (string.IsNullOrEmpty(url) || x.Disabled || x.IgnoredClient)
                    return null;
                if (string.IsNullOrEmpty(x.Database))
                    return new OperationMetadata(url, x.Username, x.Password, x.Domain, x.ApiKey);

                return new OperationMetadata(
                    MultiDatabase.GetRootDatabaseUrl(url) + "/databases/" + x.Database + "/",
                    x.Username,
                    x.Password,
                    x.Domain,
                    x.ApiKey);
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

            if (replicationDocument.ClientConfiguration != null)
                Conventions.UpdateFrom(replicationDocument.ClientConfiguration);
        }

        protected override string GetServerCheckUrl(string baseUrl)
        {
            return baseUrl + "/replication/topology?check-server-reachable";
        }

        public void RefreshReplicationInformation(AsyncServerClient serverClient)
        {
            RefreshReplicationInformationInternal(serverClient.Url, () => AsyncHelpers.RunSync(() => serverClient.DirectGetReplicationDestinationsAsync(new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials))));
        }

        public override void RefreshReplicationInformation(ServerClient serverClient)
        {
            RefreshReplicationInformationInternal(serverClient.Url, () => serverClient.DirectGetReplicationDestinations(new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials)));
        }

        private void RefreshReplicationInformationInternal(string url, Func<ReplicationDocument> getReplicationDestinations)
        {
            lock (this)
            {
                var serverHash = ServerHash.GetServerHash(url);

                JsonDocument document;
                var fromFailoverUrls = false;

                try
                {
                    var replicationDestinations = getReplicationDestinations();
                    document = replicationDestinations == null ? null : RavenJObject.FromObject(replicationDestinations).ToJsonDocument();
                    failureCounts[url] = new FailureCounter(); // we just hit the master, so we can reset its failure count
                }
                catch (Exception e)
                {
                    log.ErrorException("Could not contact master for new replication information", e);
                    document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);

                    if (document == null)
                    {
                        if (FailoverServers != null && FailoverServers.Length > 0) // try to use configured failover servers
                        {
                            var failoverServers = new ReplicationDocument { Destinations = new List<ReplicationDestination>() };

                            foreach (var failover in FailoverServers)
                            {
                                failoverServers.Destinations.Add(failover);
                            }

                            document = new JsonDocument
                                       {
                                           DataAsJson = RavenJObject.FromObject(failoverServers)
                                       };

                            fromFailoverUrls = true;
                        }
                    }
                }


                if (document == null)
                {
                    LastReplicationUpdate = SystemTime.UtcNow; // checked and not found
                    ReplicationDestinations = new List<OperationMetadata>(); // clear destinations that could be retrieved from local storage, but do not use clear because we might be in a middle of enumeration
                    return;
                }

                if (!fromFailoverUrls)
                    ReplicationInformerLocalCache.TrySavingReplicationInformationToLocalCache(serverHash, document);

                UpdateReplicationInformationFromDocument(document);

                LastReplicationUpdate = SystemTime.UtcNow;
            }
        }
    }
}
