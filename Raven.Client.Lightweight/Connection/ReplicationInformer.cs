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
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	public class ReplicationInformer : ReplicationInformerBase<ServerClient>, IDocumentStoreReplicationInformer
	{
        private const string RavenReplicationDestinations = "Raven/Replication/Destinations";

        public ReplicationInformer(Convention conventions) : base(conventions)
        {
        }

        /// <summary>
        /// Failover servers set manually in config file or when document store was initialized
        /// </summary>
        public ReplicationDestination[] FailoverServers { get; set; }

        public override Task UpdateReplicationInformationIfNeeded(ServerClient serverClient)
        {
            if (conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
                return new CompletedTask();

            if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
                return new CompletedTask();

            lock (replicationLock)
            {
                if (firstTime)
                {
                    var serverHash = ServerHash.GetServerHash(serverClient.Url);

                    var document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
                    if (IsInvalidDestinationsDocument(document) == false)
                    {
                        UpdateReplicationInformationFromDocument(document);
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
        }

        public override void RefreshReplicationInformation(ServerClient commands)
        {
            lock (this)
            {
                var serverHash = ServerHash.GetServerHash(commands.Url);

                JsonDocument document;
                var fromFailoverUrls = false;

                try
                {
                    document = commands.DirectGet(new OperationMetadata(commands.Url, commands.PrimaryCredentials), RavenReplicationDestinations);
                    failureCounts[commands.Url] = new FailureCounter(); // we just hit the master, so we can reset its failure count
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

                            document = new JsonDocument();
                            document.DataAsJson = RavenJObject.FromObject(failoverServers);

                            fromFailoverUrls = true;
                        }
                    }
                }


                if (document == null)
                {
                    lastReplicationUpdate = SystemTime.UtcNow; // checked and not found
                    return;
                }

                if (!fromFailoverUrls)
                    ReplicationInformerLocalCache.TrySavingReplicationInformationToLocalCache(serverHash, document);

                UpdateReplicationInformationFromDocument(document);

                lastReplicationUpdate = SystemTime.UtcNow;
            }
        }
    }
}