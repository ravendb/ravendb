// -----------------------------------------------------------------------
//  <copyright file="RaftTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net.Http;

using Rachis.Transport;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Raft;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Helpers;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Remoting.Channels;
using System.Threading;
using Rachis;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Request;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Raft
{
    public class RaftTestBase : RavenTest
    {
        private const int PortRangeStart = 9000;

        private static int numberOfPortRequests;

        internal static int GetPort()
        {
            var portRequest = Interlocked.Increment(ref numberOfPortRequests);
            return PortRangeStart - (portRequest % 25);
        }

        public static IEnumerable<object[]> Nodes
        {
            get
            {
                return new[]
                {
                    new object[] {1},
                    new object[] {3},
                    new object[] {5}
                };
            }
        }

        public void WaitForDelete(IDatabaseCommands commands, string key, TimeSpan? timeout = null)
        {
            var done = SpinWait.SpinUntil(() =>
            {
                // We expect to get the doc from the <system> database
                var doc = commands.Get(key);
                return doc == null;
            }, timeout ?? TimeSpan.FromMinutes(5));

            if (!done)
                throw new Exception("WaitForDelete failed");
        }

        public void WaitFor(IDatabaseCommands commands, Func<IDatabaseCommands, bool> action, TimeSpan? timeout = null)
        {
            var done = SpinWait.SpinUntil(() => action(commands), timeout ?? TimeSpan.FromMinutes(5));

            if (!done)
                throw new Exception("WaitFor failed");
        }

        private Action<DocumentStore> defaultConfigureStore = store => store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromLeaderWriteToLeader;

        public List<DocumentStore> CreateRaftCluster(int numberOfNodes, string activeBundles = null, Action<DocumentStore> configureStore = null, [CallerMemberName] string databaseName = null, bool inMemory = true, bool fiddler = false)
        {
            if (configureStore == null)
                configureStore = defaultConfigureStore;
            var nodes = Enumerable.Range(0, numberOfNodes)
                .Select(x => GetNewServer(GetPort(), activeBundles: activeBundles, databaseName: databaseName, runInMemory: inMemory,
                    configureConfig: configuration =>
                    {
                        configuration.Cluster.ElectionTimeout *= 10;
                        configuration.Cluster.HeartbeatTimeout *= 10;
                    }))
                .ToList();

            var allNodesFinishedJoining = new ManualResetEventSlim();

            var random = new Random();
            var leader = nodes[random.Next(0, numberOfNodes - 1)];

            leader.Options.ClusterManager.Value.InitializeTopology(forceCandidateState: true);

            Assert.True(leader.Options.ClusterManager.Value.Engine.WaitForLeader(), "Leader was not elected by himself in time");

            leader.Options.ClusterManager.Value.Engine.TopologyChanged += command =>
            {
                if (command.Requested.AllNodeNames.All(command.Requested.IsVoter))
                {
                    allNodesFinishedJoining.Set();
                }
            };

            for (var i = 0; i < numberOfNodes; i++)
            {
                var n = nodes[i];

                if (n == leader)
                    continue;

                Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(new NodeConnectionInfo
                {
                    Name = RaftHelper.GetNodeName(n.SystemDatabase.TransactionalStorage.Id),
                    Uri = RaftHelper.GetNodeUrl(n.SystemDatabase.Configuration.ServerUrl)
                }).Wait(3000), "Failed to add node to cluster");
            }

            if (numberOfNodes == 1)
                allNodesFinishedJoining.Set();

            Assert.True(allNodesFinishedJoining.Wait(10000 * numberOfNodes), "Not all nodes become voters. " + leader.Options.ClusterManager.Value.Engine.CurrentTopology);
            Assert.True(leader.Options.ClusterManager.Value.Engine.WaitForLeader(), "Wait for leader timeout");

            WaitForClusterToBecomeNonStale(nodes);

            foreach (var node in nodes)
            {
                var url = node.SystemDatabase.ServerUrl.ForDatabase(databaseName);
                var serverHash = ServerHash.GetServerHash(url);
                ReplicationInformerLocalCache.ClearReplicationInformationFromLocalCache(serverHash);
            }

            var documentStores = nodes
                .Select(node => NewRemoteDocumentStore(ravenDbServer: node, fiddler: fiddler, activeBundles: activeBundles, configureStore: configureStore, databaseName: databaseName))
                .ToList();

            foreach (var documentStore in documentStores)
            {
                ((ClusterAwareRequestExecuter) ((ServerClient) documentStore.DatabaseCommands).RequestExecuter).WaitForLeaderTimeout = TimeSpan.FromSeconds(30);
            }
            return documentStores;
        }

        public List<DocumentStore> ExtendRaftCluster(int numberOfExtraNodes, Guid topologyId, string activeBundles = null, Action<DocumentStore> configureStore = null, [CallerMemberName] string databaseName = null, bool inMemory = true)
        {
            if (configureStore == null)
                configureStore = defaultConfigureStore;
            var leader = ChooseTheRealLeader(topologyId);
            Assert.NotNull(leader);

            var nodes = Enumerable.Range(0, numberOfExtraNodes)
                .Select(x => GetNewServer(GetPort(), activeBundles: activeBundles, databaseName: databaseName, runInMemory: inMemory,
                    configureConfig: configuration =>
                    {
                        configuration.Cluster.ElectionTimeout *= 10;
                        configuration.Cluster.HeartbeatTimeout *= 10;
                    }))
                .ToList();

            var allNodesFinishedJoining = new ManualResetEventSlim();
            leader.Options.ClusterManager.Value.Engine.TopologyChanged += command =>
            {
                if (command.Requested.AllNodeNames.All(command.Requested.IsVoter))
                {
                    allNodesFinishedJoining.Set();
                }
            };

            for (var i = 0; i < numberOfExtraNodes; i++)
            {
                var n = nodes[i];

                if (n == leader)
                    continue;

                Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(new NodeConnectionInfo
                {
                    Name = RaftHelper.GetNodeName(n.SystemDatabase.TransactionalStorage.Id),
                    Uri = RaftHelper.GetNodeUrl(n.SystemDatabase.Configuration.ServerUrl)
                }).Wait(10000));
                Assert.True(allNodesFinishedJoining.Wait(10000), "Not all nodes finished joining");
                allNodesFinishedJoining.Reset();
            }

            return nodes
                .Select(node => NewRemoteDocumentStore(ravenDbServer: node, activeBundles: activeBundles, configureStore: configureStore, databaseName: databaseName))
                .ToList();
        }

        public void RemoveFromCluster(RavenDbServer serverToRemove, Guid topologyId)
        {
            //any tests that fails because of this is invalid.
            if (servers.Count <= 2)
                throw new InvalidOperationException("Can't remove node from cluster when there are two nodes in the cluster, you need to brutly remove the node.");
            var leader = ChooseTheRealLeader(topologyId);
            if (leader == null)
                throw new InvalidOperationException("Leader is currently not present, thus can't remove node from cluster");
            if (leader == serverToRemove)
            {
                leader.Options.ClusterManager.Value.Engine.StepDownAsync().Wait();
                leader.Server.Options.ClusterManager.Value.Engine.WaitForLeader();
                leader = ChooseTheRealLeader(topologyId);
                leader.Server.Options.ClusterManager.Value.Engine.WaitForLeaderConfirmed();
                //this is because a leader chosen event is placed wrongly
                //SpinWait.SpinUntil(()=>leader.Options.ClusterManager.Value.Engine.PersistentState.GetLogEntry(leader.Options.ClusterManager.Value.Engine.CommitIndex).Term
                //                 == leader.Options.ClusterManager.Value.Engine.PersistentState.CurrentTerm,TimeSpan.FromSeconds(10));
            }
            leader.Options.ClusterManager.Value.Engine.RemoveFromClusterAsync(serverToRemove.Options.ClusterManager.Value.Engine.Options.SelfConnection).Wait(10000);
        }

        private RavenDbServer ChooseTheRealLeader(Guid topologyId)
        {
            return servers.OrderByDescending(server => server.Options.ClusterManager.Value.Engine.PersistentState.LastLogEntry().Term)
                .ThenByDescending(server => server.Options.ClusterManager.Value.Engine.PersistentState.LastLogEntry().Index)
                .FirstOrDefault(server => server.Options.ClusterManager.Value.IsLeader()
                                          && topologyId == server.Options.ClusterManager.Value.Engine.CurrentTopology.TopologyId);
        }

        private void WaitForClusterToBecomeNonStale(IReadOnlyCollection<RavenDbServer> nodes)
        {
            var numberOfNodes = nodes.Count;
            var result = SpinWait.SpinUntil(() => nodes.All(x => x.Options.ClusterManager.Value.Engine.CurrentTopology.AllVotingNodes.Count() == numberOfNodes), TimeSpan.FromSeconds(10));

            if (result == false)
                throw new InvalidOperationException("Cluster is stale.");
        }

        protected IDisposable ForceNonClusterRequests(List<DocumentStore> stores)
        {
            var conventionsRestoreInfo = stores.Select(x => new
            {
                Store = x,
                Convention = x.Conventions.FailoverBehavior
            }).ToDictionary(x => x.Store, x => x.Convention);

            stores.ForEach(store => store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately);

            return new DisposableAction(() =>
            {
                foreach (var restoreInfo in conventionsRestoreInfo)
                {
                    restoreInfo.Key.Conventions.FailoverBehavior = restoreInfo.Value;
                }
            });
        }

        protected void WaitForClusterToBecomeNonStale(int numberOfNodes)
        {
            using (var countDownEvent = new CountdownEvent(numberOfNodes))
            {
                foreach (var ravenDbServer in servers)
                {
                    ravenDbServer.Options.ClusterManager.Value.Engine.TopologyChanged += command =>
                    {
                        if (command.Requested.AllVotingNodes.Count() == numberOfNodes)
                        {
                            countDownEvent.Signal();
                        }
                    };
                }
                var clusterNotStale = countDownEvent.Wait(TimeSpan.FromSeconds(5 * numberOfNodes));
                if (clusterNotStale == false)
                {
                    clusterNotStale = true;
                    //making sure we didn't miss a topology change event that happened before this method was called
                    foreach (var ravenDbServer in servers)
                    {
                        var topology = ravenDbServer.Options.ClusterManager.Value.Engine.CurrentTopology;
                        if (topology.AllNodes.All(node => topology.IsVoter(node.Name)) == false)
                        {
                            clusterNotStale = false;
                            break;
                        }
                    }
                }
                Assert.True(clusterNotStale, $"Cluster didn't become un-stale in time");
            }
        }

        protected void SetupClusterConfiguration(List<DocumentStore> clusterStores, bool enableReplication = true, Dictionary<string, string> databaseSettings = null)
        {
      
            var clusterStore = clusterStores[0];
            var requestFactory = new HttpRavenRequestFactory();
            var replicationRequestUrl = string.Format("{0}/admin/cluster/commands/configuration", clusterStore.Url);
            var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethod.Put, new RavenConnectionStringOptions
            {
                Url = clusterStore.Url
            });
            replicationRequest.Write(RavenJObject.FromObject(new ClusterConfiguration
            {
                EnableReplication = enableReplication,
                DatabaseSettings = databaseSettings
            }));
            replicationRequest.ExecuteRequest();

            clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));
            using (ForceNonClusterRequests(clusterStores))
            {
                clusterStores.ForEach(store => WaitFor(store.DatabaseCommands, commands =>
                {
                    using (var request = commands.CreateRequest("/configuration/replication", HttpMethod.Get))
                    {
                        var replicationDocumentJson = request.ReadResponseJson() as RavenJObject;
                        if (replicationDocumentJson == null)
                            return false;

                        var replicationDocument = replicationDocumentJson.JsonDeserialization<ReplicationDocument>();
                        return replicationDocument.Destinations.Count == clusterStores.Count - 1;
                    }
                }));
            }
        }

        protected void UpdateTopologyForAllClients(IEnumerable<DocumentStore> clusterStores)
        {
            clusterStores.ForEach(store => ((ServerClient) store.DatabaseCommands).RequestExecuter.UpdateReplicationInformationIfNeededAsync((AsyncServerClient) store.AsyncDatabaseCommands, force: true));
        }
    }
}

