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
using System.Threading.Tasks;
using Rachis;
using Rachis.Commands;
using Rachis.Storage;
using Rachis.Transport;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Raft.Storage;
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

        private Timer timer = null;

        public ClusterManager(RaftEngine engine, DatabasesLandlord databasesLandlord)
        {
            Engine = engine;
            Client = new ClusterManagementHttpClient(engine);
            DatabasesLandlord = databasesLandlord;
            engine.StateChanged += OnRaftEngineStateChanged;
        }

        private DatabasesLandlord DatabasesLandlord { get; set; }

        private void OnRaftEngineStateChanged(RaftEngineState state)
        {
            if (state == RaftEngineState.Leader)
            {
                var period = DatabasesLandlord.SystemDatabase.Configuration.Cluster.MaxReplicationLatency;
                //timer should be null 
                timer?.Dispose();
                timer = new Timer(NewLeaderTimerCallback,null, period, period); 
            }
            else
            {
                //If this node was the leader before, dispose of the timer.
                timer?.Dispose();
            }
        }

        private void NewLeaderTimerCallback(object state)
        {
            //This method should never be invoked from non-leader
            //Might happen if the timer and the state change event happen at the same time.
            if (Engine.State != RaftEngineState.Leader)
            {
                if(Log.IsDebugEnabled)
                    Log.Debug($"NewLeaderTimerCallback invoked from non-leader node, actual state:{Engine.State}");
                return;
            }
            var databasesNames = GetDatabasesNames();
            var databaseToLastModified = new Dictionary<string,Tuple<DateTime,string>>();
            foreach (var databaseName in databasesNames)
            {
                try
                {
                    Task<DocumentDatabase> databaseTask;
                    if (DatabasesLandlord.TryGetOrCreateResourceStore(databaseName, out databaseTask) == false)
                        continue;
                    var database = databaseTask.Result;
                    JsonDocument lastDoc = null;
                    database.TransactionalStorage.Batch(action =>
                    {
                        lastDoc = action.Documents.GetDocumentsByReverseUpdateOrder(0, 1).FirstOrDefault();                        

                    });
                    if (lastDoc == null)
                    {
                        if (Log.IsWarnEnabled)
                            Log.Warn($"Failed to get last document for databse: {databaseName} while generating replication state");
                        continue;
                    }
                    databaseToLastModified[databaseName] = new Tuple<DateTime, string>(lastDoc.LastModified??DateTime.MinValue,database.TransactionalStorage.Id.ToString());
                }
                catch (Exception e)
                {
                    if(Log.IsWarnEnabled)
                        Log.WarnException($"Failed to get database: {databaseName} while generating replication state",e);
                }

            }
            Client.SendReplicationStateAsync(databaseToLastModified);
        }

        private HashSet<string> GetDatabasesNames()
        {
            int nextPageStart = 0;
            var databases = DatabasesLandlord.SystemDatabase.Documents
                .GetDocumentsWithIdStartingWith(DatabasesLandlord.ResourcePrefix, null, null, 0,
                    int.MaxValue, CancellationToken.None, ref nextPageStart);

            var databaseNames = databases
                .Select(database =>
                    database.Value<RavenJObject>("@metadata").Value<string>("@id").Replace(DatabasesLandlord.ResourcePrefix, string.Empty)).ToHashSet();
            return databaseNames;
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

            aggregator.Execute(() =>
            {
                timer?.Dispose();
            });
            aggregator.Execute(() =>
            {
                if(Engine != null)
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
