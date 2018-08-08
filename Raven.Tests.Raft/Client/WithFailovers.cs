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
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
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
                    documentStore.JsonRequestFactory.RequestTimeout = TimeSpan.FromSeconds(15);
                }

                SetupClusterConfiguration(clusterStores);

                clusterStores.ForEach(store => 
                    AsyncHelpers.RunSync(()=>((ServerClient)store.DatabaseCommands).RequestExecuter.UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store.AsyncDatabaseCommands, force:true)));

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


        [Theory(Skip = "Flaky")]
        [InlineData(3, FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers)]
        [InlineData(3, FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers)]
        [InlineData(5, FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers)]
        [InlineData(5, FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers)]
        public void ReadFromAllWriteToLeaderWithFailoversAndMajorityDown(int numberOfNodes, FailoverBehavior failoverBehavior)
        {
            using (WithCustomDatabaseSettings(doc => doc.Settings["Raven/Replication/ReplicationRequestTimeout"] = "4000"))
            {                
                var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: s => s.Conventions.FailoverBehavior = failoverBehavior);
                foreach (var documentStore in clusterStores)
                {
                    // set lower timeout to reduce test time
                    documentStore.JsonRequestFactory.RequestTimeout = TimeSpan.FromSeconds(15);
                }


                clusterStores.ForEach(store =>
                    AsyncHelpers.RunSync(() => ((ServerClient)store.DatabaseCommands).RequestExecuter.UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store.AsyncDatabaseCommands, force: true)));

                SetupClusterConfiguration(clusterStores);

                using (ForceNonClusterRequests(clusterStores))
                {
                    foreach (var curStore in clusterStores)
                    {
                        var systemDatabaseStore = curStore.DatabaseCommands.ForSystemDatabase();

                        JsonDocument rawRepDoc = systemDatabaseStore.Get(Constants.Global.ReplicationDestinationsDocumentName);
                        var repDoc = rawRepDoc.DataAsJson.Deserialize<ReplicationDocument>(clusterStores[0].Conventions);

                        repDoc.ClientConfiguration.FailoverBehavior = failoverBehavior;
                        repDoc.ClientConfiguration.OnlyModifyFailoverIfNotInClusterAlready = false;

                        systemDatabaseStore.Put(Constants.Global.ReplicationDestinationsDocumentName, null, RavenJObject.FromObject(repDoc), new RavenJObject());

                        clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));
                    }
                }



                clusterStores.ForEach(store =>
                    AsyncHelpers.RunSync(() => ((ServerClient)store.DatabaseCommands).RequestExecuter.UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store.AsyncDatabaseCommands, force: true)));

                for (int i = 0; i < clusterStores.Count; i++)
                {
                    var store = clusterStores[i];
                    Assert.Equal(failoverBehavior, store.Conventions.FailoverBehavior);

                    store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
                }

                using (ForceNonClusterRequests(clusterStores))
                {
                    for (int i = 0; i < clusterStores.Count; i++)
                    {
                        clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + i));
                    }
                }

                var leaderEnumerable = servers.Where(x => x.Options.ClusterManager.Value.IsLeader());
                var allServersButLeader = servers.Except(leaderEnumerable).ToList();

                for (var i = 0; i < allServersButLeader.Count - 1; i++)
                {
                    allServersButLeader[i].Dispose();
                }

                leaderEnumerable.Single().Dispose();


                var surviver = allServersButLeader[allServersButLeader.Count - 2];

                for (int i = 0; i < clusterStores.Count; i++)
                {
                    var store = clusterStores[i];

                    store.DatabaseCommands.Get("keys/" + (i + clusterStores.Count));
                }

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
