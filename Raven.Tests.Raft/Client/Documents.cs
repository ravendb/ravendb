// -----------------------------------------------------------------------
//  <copyright file="Documents.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Tasks;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Raft.Client
{
    public class Documents : RaftTestBase
    {
        [Theory]
        [PropertyData("Nodes")]
        public void CanReadFromMultipleServers1(int numberOfNodes)
        {
            CanReadFromMultipleServersInternal(numberOfNodes, FailoverBehavior.ReadFromAllWriteToLeader);
        }

        [Theory]
        [PropertyData("Nodes")]
        public void CanReadFromMultipleServers2(int numberOfNodes)
        {
            CanReadFromMultipleServersInternal(numberOfNodes, FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers);
        }

        private void CanReadFromMultipleServersInternal(int numberOfNodes, FailoverBehavior failoverBehavior)
        {
            var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.FailoverBehavior = failoverBehavior);

            SetupClusterConfiguration(clusterStores);
            Enumerable.Range(0,numberOfNodes).ForEach(i=> clusterStores[0].DatabaseCommands.Put($"keys/{i}", null, new RavenJObject(), new RavenJObject()));

            using (ForceNonClusterRequests(clusterStores))
            {
                clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, $"keys/{numberOfNodes - 1}"));
            }

            //Here i want to fetch the topology after a leader was elected so all stores will have latest topology.
            clusterStores.ForEach(store =>
            {
                var client = ((AsyncServerClient)store.AsyncDatabaseCommands);
                AsyncHelpers.RunSync(()=>client.RequestExecuter.UpdateReplicationInformationIfNeededAsync(client));
            });
            var tasks = new List<ReplicationTask>();
            foreach (var server in servers)
            {
                server.Options.DatabaseLandlord.ForAllDatabases(database => tasks.Add(database.StartupTasks.OfType<ReplicationTask>().First()));
                server.Options.ClusterManager.Value.Engine.Dispose();
            }

            foreach (var task in tasks)
            {
                task.Pause();
                SpinWait.SpinUntil(() => task.IsRunning == false, TimeSpan.FromSeconds(3));
            }

            servers.ForEach(server => server.Options.RequestManager.ResetNumberOfRequests());

            using (ForceNonClusterRequests(clusterStores))
            {
                for (int i = 0; i < clusterStores.Count; i++)
                {
                    var store = clusterStores[i];
                    Enumerable.Range(0, numberOfNodes).ForEach(j => store.DatabaseCommands.Get($"keys/{j}"));
                }
            }

            servers.ForEach(server =>
            {
                Assert.True(server.Options.RequestManager.NumberOfRequests >= numberOfNodes);
            });
        }

        [Theory]
        [PropertyData("Nodes")]
        public void PutShouldBePropagated(int numberOfNodes)
        {
            var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromLeaderWriteToLeader);

            SetupClusterConfiguration(clusterStores);

            for (int i = 0; i < clusterStores.Count; i++)
            {
                var store = clusterStores[i];

                store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
            }

            using (ForceNonClusterRequests(clusterStores))
            {
                for (int i = 0; i < clusterStores.Count; i++)
                {
                    clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + i));
                }
            }    
        }

        [Theory]
        [PropertyData("Nodes")]
        public void DeleteShouldBePropagated(int numberOfNodes)
        {
            var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromLeaderWriteToLeader);

            SetupClusterConfiguration(clusterStores);

            for (int i = 0; i < clusterStores.Count; i++)
            {
                var store = clusterStores[i];

                store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
            }

            using (ForceNonClusterRequests(clusterStores))
            {
                for (int i = 0; i < clusterStores.Count; i++)
                {
                    clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + i));
                }
            }

            for (int i = 0; i < clusterStores.Count; i++)
            {
                var store = clusterStores[i];

                store.DatabaseCommands.Delete("keys/" + i, null);
            }


            using (ForceNonClusterRequests(clusterStores))
            {
                for (int i = 0; i < clusterStores.Count; i++)
                {
                    clusterStores.ForEach(store => WaitForDelete(store.DatabaseCommands, "keys/" + i));
                }
            }
        }
    }
}
