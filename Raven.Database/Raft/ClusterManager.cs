// -----------------------------------------------------------------------
//  <copyright file="ClusterManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Rachis;
using Rachis.Commands;
using Rachis.Storage;
using Rachis.Transport;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Bundles.Replication.Data;
using Raven.Client.Connection;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Raft.Dto;
using Raven.Database.Server.Tenancy;
using Raven.Json.Linq;

namespace Raven.Database.Raft
{
    public class ClusterManager : IDisposable
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        public RaftEngine Engine { get; private set; }

        public ClusterManagementHttpClient Client { get; private set; }

        public bool HasNonEmptyTopology => Engine.CurrentTopology.AllNodes.Any();

        private Timer timer;

        public ClusterManager(RaftEngine engine, DatabasesLandlord databasesLandlord)
        {
            Engine = engine;
            Client = new ClusterManagementHttpClient(engine);
            DatabasesLandlord = databasesLandlord;
            engine.StateChanged += OnRaftEngineStateChanged;
            engine.ProposingCandidacy += OnProposingCandidacy;
            maxReplicationLatency = DatabasesLandlord.SystemDatabase.Configuration.Cluster.MaxReplicationLatency;
            clusterManagerStartTime = DateTime.UtcNow;
        }

        private readonly TimeSpan maxReplicationLatency;
        private readonly DateTime clusterManagerStartTime;
        private void OnProposingCandidacy(object sender, ProposingCandidacyResult e)
        {
            var replicationStateDoc = DatabasesLandlord.SystemDatabase.Documents.Get(Constants.Cluster.ClusterReplicationStateDocumentKey,null);
            if (replicationStateDoc == null)
            {
                //This is a case of a node loading for the first time and just never got any replication state.
                //If we prevent this than a cluster will be non-respnosive when loaded (mostly a test senario but could be a real issue)
                if (clusterManagerStartTime + maxReplicationLatency + maxReplicationLatency < DateTime.UtcNow)
                {
                    e.VetoCandidacy = true;
                    e.Reason = "Could not find replication state document";                    
                }
                return;
            }
            var replicationState = replicationStateDoc.DataAsJson.ToObject<ReplicationState>();
            if (replicationState == null)
            {
                e.VetoCandidacy = true;
                e.Reason = "Could not deserialize replication state document";
                return;
            }

            var anyDatabaseUptodate = false;
            //preventing the case where all databases are inactive (long overnight inactivity).
            var anyDatabaseUp = false;
            DatabasesLandlord.ForAllDatabases(database =>
            {
                anyDatabaseUp = true;
                LastModificationTimeAndTransactionalId modification;
                //if the source document doesn't contain this databse it means it was not active in the source
                //nothing we can do but ignore this.
                if (replicationState.DatabasesToLastModification.TryGetValue(database.Name, out modification) == false)
                    return;
                var docKey = $"{Constants.RavenReplicationSourcesBasePath}/{modification.DatabaseId}";
                var doc = database.Documents.Get(docKey, null);
                if (doc == null)
                    return;
                var sourceInformation = doc.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
                if (sourceInformation == null)
                    return;
                var lastUpdate = sourceInformation.LastModifiedAtSource ?? DateTime.MinValue;                
                if (lastUpdate + maxReplicationLatency >= modification.LastModified)
                    anyDatabaseUptodate = true;
            },true);
            if (anyDatabaseUptodate == false && anyDatabaseUp)
            {
                e.VetoCandidacy = true;
                e.Reason = "None of the active databases are up to date with the leader last replication state";
            }
        }

        private DatabasesLandlord DatabasesLandlord { get; set; }

        private void OnRaftEngineStateChanged(RaftEngineState state)
        {
            if (state == RaftEngineState.Leader)
            {
                var period = DatabasesLandlord.SystemDatabase.Configuration.Cluster.MaxReplicationLatency;
                //timer should be null 
                SaflyDisposeOfTimer();
                timer = new Timer(NewLeaderTimerCallback,null, period, period); 
            }
            else
            {
                //If this node was the leader before, dispose of the timer.
                SaflyDisposeOfTimer();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SaflyDisposeOfTimer()
        {
            var timerSnapshot = timer;
            timerSnapshot?.Dispose();
            timer = null;
        }

        private static int newLeaderTimerCallbackLocker;
        private void NewLeaderTimerCallback(object state)
        {
            //preventing multiple calls to the callback
            if (Interlocked.CompareExchange(ref newLeaderTimerCallbackLocker, 1, 0) == 1)
                return;
            try
            {
                //This method should never be invoked from non-leader
                //Might happen if the timer and the state change event happen at the same time.
                if (Engine.State != RaftEngineState.Leader)
                {
                    if(Log.IsDebugEnabled)
                        Log.Debug($"NewLeaderTimerCallback invoked from non-leader node, actual state:{Engine.State}");
                    return;
                }
                var databaseToLastModified = new Dictionary<string, LastModificationTimeAndTransactionalId>();
                DatabasesLandlord.ForAllDatabases(db => 
                {
                    try
                    {
                        var databaseName = db.Name;
                        JsonDocument lastDoc = null;
                        db.TransactionalStorage.Batch(action =>
                        {
                            lastDoc = action.Documents.GetDocumentsByReverseUpdateOrder(0, 1).FirstOrDefault();                        

                        });
                        var lastModified = lastDoc?.LastModified ?? DateTime.MinValue;
                        databaseToLastModified[databaseName] = new LastModificationTimeAndTransactionalId
                        {
                            LastModified = lastModified,
                            DatabaseId = db.TransactionalStorage.Id.ToString()
                        };
                    }
                    catch (Exception e)
                    {
                        if(Log.IsWarnEnabled)
                            Log.WarnException($"Failed to get database: {db.Name} while generating replication state",e);
                    }

                },true);
                Client.SendReplicationStateAsync(new ReplicationState(databaseToLastModified));
            }
            finally
            {
                //releasing the 'lock'
                Interlocked.Exchange(ref newLeaderTimerCallbackLocker, 0);
            }
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

        public void InitializeTopology(NodeConnectionInfo nodeConnection = null, bool isPartOfExistingCluster = false, bool forceCandidateState = false)
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

            aggregator.Execute(SaflyDisposeOfTimer);
             aggregator.Execute(() =>
             {
                 if (Engine != null && disableReplicationStateChecks == false)
                     Engine.StateChanged -= OnRaftEngineStateChanged;
             });
            aggregator.Execute(() =>
            {
                Client?.Dispose();
            });

            aggregator.Execute(() =>
            {
                Engine?.Dispose();
            });

            aggregator.ThrowIfNeeded();
        }

        private bool disableReplicationStateChecks;
        private int disableReplicationStateChecksLocker;
        public void HandleClusterConfigurationChanged(ClusterConfiguration configuration)
        {
            if (Interlocked.CompareExchange(ref disableReplicationStateChecksLocker, 1, 0) == 1)
                return;
            try
            {
                //same value nothing to do
                if (configuration.DisableReplicationStateChecks == disableReplicationStateChecks)
                    return;
                if (configuration.DisableReplicationStateChecks)
                {
                    Engine.StateChanged -= OnRaftEngineStateChanged;
                    SaflyDisposeOfTimer();
                }
                else
                {
                    Engine.StateChanged += OnRaftEngineStateChanged;
                }
                disableReplicationStateChecks = configuration.DisableReplicationStateChecks;
            }
            finally
            {
                Interlocked.Exchange(ref disableReplicationStateChecksLocker, 0);
            }
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
