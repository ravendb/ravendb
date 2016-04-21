// -----------------------------------------------------------------------
//  <copyright file="ClusterManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Rachis;
using Rachis.Commands;
using Rachis.Storage;
using Rachis.Transport;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Json.Linq;

namespace Raven.Database.Raft
{
    public class ClusterManager : IDisposable
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        public RaftEngine Engine { get; private set; }

        public ClusterManagementHttpClient Client { get; private set; }

        public ClusterManager(RaftEngine engine)
        {
            Engine = engine;
            Client = new ClusterManagementHttpClient(engine);
        }

        public ClusterTopology GetTopology()
        {
            return new ClusterTopology
                   {
                       CurrentLeader = Engine.CurrentLeader,
                       CurrentTerm = Engine.PersistentState.CurrentTerm,
                       State = Engine.State.ToString(),
                       CommitIndex = Engine.CommitIndex,
                       AllVotingNodes = Engine.CurrentTopology.AllVotingNodes.ToArray(),
                       PromotableNodes = Engine.CurrentTopology.PromotableNodes.ToArray(),
                       NonVotingNodes = Engine.CurrentTopology.NonVotingNodes.ToArray(),
                       TopologyId = Engine.CurrentTopology.TopologyId
                   };
        }

        public void InitializeTopology(NodeConnectionInfo nodeConnection = null, bool isPartOfExistingCluster = false,bool forceCandidateState = false)
        {
            var topologyId = Guid.NewGuid();
            var topology = new Topology(topologyId, new List<NodeConnectionInfo> { nodeConnection ?? Engine.Options.SelfConnection }, Enumerable.Empty<NodeConnectionInfo>(), Enumerable.Empty<NodeConnectionInfo>());

            var tcc = new TopologyChangeCommand
            {
                Requested = topology,
                Previous = isPartOfExistingCluster ? Engine.CurrentTopology : null
            };

            Engine.PersistentState.SetCurrentTopology(tcc.Requested, 0);
            Engine.StartTopologyChange(tcc);
            Engine.CommitTopologyChange(tcc);

            if (isPartOfExistingCluster || forceCandidateState)
                Engine.ForceCandidateState();
            else
                Engine.CurrentLeader = null;

            Log.Info("Initialized Topology: " + topologyId);
        }

        public void InitializeEmptyTopologyWithId(Guid id)
        {
            var tcc = new TopologyChangeCommand
            {
                Requested = new Topology(id),
                Previous = Engine.CurrentTopology
            };

            Engine.PersistentState.SetCurrentTopology(tcc.Requested, 0);
            Engine.StartTopologyChange(tcc);
            Engine.CommitTopologyChange(tcc);

            Log.Info("Changed topology id: " + id + " and set the empty cluster topology");
        }

        public void CleanupAllClusteringData(DocumentDatabase systemDatabase)
        {
            // dispose cluster manager
            Dispose();

            // delete Raft Storage
            var voronDataPath = Path.Combine(systemDatabase.Configuration.DataDirectory ?? AppDomain.CurrentDomain.BaseDirectory, "Raft");
            IOExtensions.DeleteDirectory(voronDataPath);

            // delete last applied commit
            systemDatabase.TransactionalStorage.Batch(accessor =>
            {
                accessor.Lists.Remove("Raven/Cluster", "Status");
            });

            // delete Raven-Non-Cluster-Database markers from databases settings
            int nextStart = 0;
            var databases = systemDatabase
                .Documents
                .GetDocumentsWithIdStartingWith(Constants.Database.Prefix, null, null, 0, int.MaxValue, systemDatabase.WorkContext.CancellationToken, ref nextStart);

            foreach (var database in databases)
            {
                var settings = database.Value<RavenJObject>("Settings");
                
                if (settings != null && settings.ContainsKey(Constants.Cluster.NonClusterDatabaseMarker))
                {
                    settings.Remove(Constants.Cluster.NonClusterDatabaseMarker);
                    var jsonDocument = ((RavenJObject)database).ToJsonDocument();
                    systemDatabase.Documents.Put(jsonDocument.Key, jsonDocument.Etag, jsonDocument.DataAsJson, jsonDocument.Metadata, null);
                }
            }

        }

        public void Dispose()
        {
            var aggregator = new ExceptionAggregator("ClusterManager disposal error.");

            aggregator.Execute(() =>
            {
                if (Client != null)
                    Client.Dispose();
            });

            aggregator.Execute(() =>
            {
                if (Engine != null)
                    Engine.Dispose();
            });

            aggregator.ThrowIfNeeded();
        }
    }

    public class ClusterTopology
    {
        public string CurrentLeader { get; set; }

        public long CurrentTerm { get; set; }

        public string State { get; set; }

        public long CommitIndex { get; set; }

        public NodeConnectionInfo[] AllVotingNodes { get; set; }

        public NodeConnectionInfo[] PromotableNodes { get; set; }

        public NodeConnectionInfo[] NonVotingNodes { get; set; }

        public Guid TopologyId { get; set; }
    }
}
