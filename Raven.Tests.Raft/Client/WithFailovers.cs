// -----------------------------------------------------------------------
//  <copyright file="WithFailovers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using Rachis;
using Raven.Abstractions;
using Raven.Abstractions.Cluster;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Database.Config;
using Raven.Database.Raft.Util;
using Raven.Json.Linq;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Raft.Client
{
    public class WithFailovers : RaftTestBase
    {
        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.Replication.ReplicationRequestTimeoutInMilliseconds = 4000;
        }

        [Theory]
        [InlineData(3)]
        [InlineData(5)]
        public void ReadFromLeaderWriteToLeaderWithFailoversShouldWork(int numberOfNodes)
        {
            WithFailoversInternal(numberOfNodes, FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers);
        }

        [Theory]
        [InlineData(3)]
        [InlineData(5)]
        public void ReadFromAllWriteToLeaderWithFailoversShouldWork(int numberOfNodes)
        {
            WithFailoversInternal(numberOfNodes, FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers);
        }

        private void WithFailoversInternal(int numberOfNodes, FailoverBehavior failoverBehavior)
        {
            using (WithCustomDatabaseSettings(doc => doc.Settings["Raven/Replication/ReplicationRequestTimeout"] = "4000"))
            {
                var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: s => s.Conventions.FailoverBehavior = failoverBehavior);

                foreach (var documentStore in clusterStores)
                {
                    // set lower timeout to reduce test time
                    documentStore.JsonRequestFactory.RequestTimeout = TimeSpan.FromSeconds(5);
                }

                SetupClusterConfiguration(clusterStores);

                clusterStores.ForEach(store => ((ServerClient)store.DatabaseCommands).RequestExecuter.UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store.AsyncDatabaseCommands, force:true));

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

                var oldLeader = servers.First(x => x.Options.ClusterManager.Value.IsLeader());
                oldLeader.Dispose();

                for (int i = 0; i < clusterStores.Count; i++)
                {
                    var store = clusterStores[i];

                    store.DatabaseCommands.Put("keys/" + (i + clusterStores.Count), null, new RavenJObject(), new RavenJObject());
                }

                for (int i = 0; i < clusterStores.Count; i++)
                {
                    clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + (i + clusterStores.Count)));
                }
            }
        }
    }
}
