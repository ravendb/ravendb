// -----------------------------------------------------------------------
//  <copyright file="Attachments.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Replication;
using Raven.Json.Linq;

using Xunit.Extensions;

namespace Raven.Tests.Raft.Client
{
    public class Attachments : RaftTestBase
    {
        [Theory]
        [PropertyData("Nodes")]
        public void PutShouldBePropagated(int numberOfNodes)
        {
            var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromLeaderWriteToLeader);

            SetupClusterConfiguration(clusterStores);

            for (int i = 0; i < clusterStores.Count; i++)
            {
                var store = clusterStores[i];

#pragma warning disable 618
                store.DatabaseCommands.PutAttachment("keys/" + i, null, new MemoryStream(), new RavenJObject());
#pragma warning restore 618
            }

            using (ForceNonClusterRequests(clusterStores))
            {
                for (int i = 0; i < clusterStores.Count; i++)
                {
#pragma warning disable 618
                    clusterStores.ForEach(store => WaitFor(store.DatabaseCommands, commands => commands.GetAttachment("keys/" + i) != null));
#pragma warning restore 618
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

#pragma warning disable 618
                store.DatabaseCommands.PutAttachment("keys/" + i, null, new MemoryStream(), new RavenJObject());
#pragma warning restore 618
            }

            using (ForceNonClusterRequests(clusterStores))
            {
                for (int i = 0; i < clusterStores.Count; i++)
                {
#pragma warning disable 618
                    clusterStores.ForEach(store => WaitFor(store.DatabaseCommands, commands => commands.GetAttachment("keys/" + i) != null));
#pragma warning restore 618
                }
            }
            

            for (int i = 0; i < clusterStores.Count; i++)
            {
                var store = clusterStores[i];

#pragma warning disable 618
                store.DatabaseCommands.DeleteAttachment("keys/" + i, null);
#pragma warning restore 618
            }

            using (ForceNonClusterRequests(clusterStores))
            {
                for (int i = 0; i < clusterStores.Count; i++)
                {
#pragma warning disable 618
                    clusterStores.ForEach(store => WaitFor(store.DatabaseCommands, commands => commands.GetAttachment("keys/" + i) == null));
#pragma warning restore 618
                }
            }
        }
    }
}
