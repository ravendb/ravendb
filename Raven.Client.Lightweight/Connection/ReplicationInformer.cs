// -----------------------------------------------------------------------
//  <copyright file="ReplicationInformation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Request;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Metrics;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
    public class ReplicationInformer : ReplicationInformerBase<ServerClient>, IDocumentStoreReplicationInformer
    {
        private readonly SemaphoreSlim replicationLock = new SemaphoreSlim(1);

        private bool firstTime = true;

        private DateTime lastReplicationUpdate = DateTime.MinValue;

        private Task refreshReplicationInformationTask;

        public ReplicationInformer(DocumentConvention conventions, HttpJsonRequestFactory jsonRequestFactory, Func<string, IRequestTimeMetric> requestTimeMetricGetter)
            : base(conventions, jsonRequestFactory, requestTimeMetricGetter)
        {
        }

        /// <summary>
        /// Failover servers set manually in config file or when document store was initialized
        /// </summary>
        public ReplicationDestination[] FailoverServers { get; set; }

        public Task UpdateReplicationInformationIfNeededAsync(AsyncServerClient serverClient, bool force = false)
        {
            return UpdateReplicationInformationIfNeededInternalAsync(serverClient.Url,
                () => serverClient.DirectGetReplicationDestinationsAsync(new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials, null), null), force);
        }

        private async Task UpdateReplicationInformationIfNeededInternalAsync(string url, Func<Task<ReplicationDocumentWithClusterInformation>> getReplicationDestinations, bool force)
        {
            if (force == false)
            {
                if (Conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
                    return;

                if (lastReplicationUpdate.Add(Conventions.TimeToWaitBetweenReplicationTopologyUpdates) > SystemTime.UtcNow)
                    return;
            }

            try
            {
                await replicationLock.WaitAsync().ConfigureAwait(false);

                if (firstTime && force == false)
                {
                    var serverHash = ServerHash.GetServerHash(url);

                    var document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
                    if (IsInvalidDestinationsDocument(document) == false)
                        UpdateReplicationInformationFromDocument(document);
                }

                firstTime = false;

                if (lastReplicationUpdate.Add(Conventions.TimeToWaitBetweenReplicationTopologyUpdates) > SystemTime.UtcNow && force == false)
                    return;

                if (refreshReplicationInformationTask != null)
                    await refreshReplicationInformationTask.ConfigureAwait(false);

                refreshReplicationInformationTask = RefreshReplicationInformationInternalAsync(url, getReplicationDestinations).
                    ContinueWith(task =>
                    {
                        if (task.Exception != null)
                            Log.ErrorException("Failed to refresh replication information", task.Exception);
                        refreshReplicationInformationTask = null;
                    }, TaskContinuationOptions.ExecuteSynchronously);
            }
            finally
            {
                replicationLock.Release();
            }
        }

        public override void ClearReplicationInformationLocalCache(ServerClient client)
        {
            var serverHash = ServerHash.GetServerHash(client.Url);
            ReplicationInformerLocalCache.ClearReplicationInformationFromLocalCache(serverHash);
        }

        public override void UpdateReplicationInformationFromDocument(JsonDocument document)
        {
            var replicationDocument = document.DataAsJson.JsonDeserialization<ReplicationDocumentWithClusterInformation>();
            ReplicationDestinations = replicationDocument.Destinations.Select(x =>
            {
                var url = string.IsNullOrEmpty(x.ClientVisibleUrl) ? x.Url : x.ClientVisibleUrl;
                if (string.IsNullOrEmpty(url))
                    return null;
                if (x.CanBeFailover() == false)
                    return null;
                if (string.IsNullOrEmpty(x.Database))
                    return new OperationMetadata(url, x.Username, x.Password, x.Domain, x.ApiKey, x.ClusterInformation);

                return new OperationMetadata(
                    MultiDatabase.GetRootDatabaseUrl(url) + "/databases/" + x.Database + "/",
                    x.Username,
                    x.Password,
                    x.Domain,
                    x.ApiKey,
                    x.ClusterInformation);
            })
                // filter out replication destination that don't have the url setup, we don't know how to reach them
                // so we might as well ignore them. Probably private replication destination (using connection string names only)
                .Where(x => x != null)
                .ToList();
            foreach (var replicationDestination in ReplicationDestinations)
            {
                FailureCounter value;
                if (FailureCounters.FailureCounts.TryGetValue(replicationDestination.Url, out value))
                    continue;
                FailureCounters.FailureCounts[replicationDestination.Url] = new FailureCounter();
            }

            if (replicationDocument.ClientConfiguration != null)
                Conventions.UpdateFrom(replicationDocument.ClientConfiguration);
            lastReplicationUpdate = DateTime.UtcNow;
        }

        protected override string GetServerCheckUrl(string baseUrl)
        {
            return baseUrl + "/replication/topology?check-server-reachable";
        }

        public void RefreshReplicationInformation(AsyncServerClient serverClient)
        {
            AsyncHelpers.RunSync(() =>
                RefreshReplicationInformationInternalAsync(serverClient.Url, () => serverClient.DirectGetReplicationDestinationsAsync(new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials, null), null)));
        }

        public override void RefreshReplicationInformation(ServerClient serverClient)
        {
            AsyncHelpers.RunSync(() =>
                RefreshReplicationInformationInternalAsync(serverClient.Url, () => Task.FromResult(serverClient.DirectGetReplicationDestinations(new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials, null), null))));
        }

        private async Task RefreshReplicationInformationInternalAsync(string url, Func<Task<ReplicationDocumentWithClusterInformation>> getReplicationDestinations)
        {
            try
            {
                await replicationLock.WaitAsync().ConfigureAwait(false);

                var serverHash = ServerHash.GetServerHash(url);

                JsonDocument document;
                var fromFailoverUrls = false;

                try
                {
                    var replicationDestinations = await getReplicationDestinations().ConfigureAwait(false);
                    document = replicationDestinations == null ? null : RavenJObject.FromObject(replicationDestinations).ToJsonDocument();
                    FailureCounters.FailureCounts[url] = new FailureCounter(); // we just hit the master, so we can reset its failure count
                }
                catch (Exception e)
                {
                    Log.ErrorException("Could not contact master for new replication information", e);
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
                    lastReplicationUpdate = SystemTime.UtcNow; // checked and not found
                    ReplicationDestinations = new List<OperationMetadata>(); // clear destinations that could be retrieved from local storage
                    Log.Info("Replication destinations cleared for url " + url + ". Failover servers count: " + (FailoverServers?.Length??0));
                    return;
                }

                if (!fromFailoverUrls)
                    ReplicationInformerLocalCache.TrySavingReplicationInformationToLocalCache(serverHash, document);

                UpdateReplicationInformationFromDocument(document);

                lastReplicationUpdate = SystemTime.UtcNow;
            }
            finally
            {
                replicationLock.Release();
            }
        }

        public override void Dispose()
        {
            base.Dispose();

            var replicationInformationTaskCopy = refreshReplicationInformationTask;
            if (replicationInformationTaskCopy != null)
                replicationInformationTaskCopy.Wait();
    }
}}
