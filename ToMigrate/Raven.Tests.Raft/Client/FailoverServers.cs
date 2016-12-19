// -----------------------------------------------------------------------
//  <copyright file="FailoverServers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Cluster;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Json.Linq;

using Xunit;

namespace Raven.Tests.Raft.Client
{
    public class FailoverServers : RaftTestBase
    {
        [Fact]
        public void ClientShouldHandleFailoverServers()
        {
            var clusterStores = CreateRaftCluster(1, activeBundles: "Replication");

            SetupClusterConfiguration(clusterStores);

            using (var store = new DocumentStore
                        {
                            Url = "http://localhost:12345/",
                            DefaultDatabase = clusterStores[0].DefaultDatabase,
                            Conventions =
                            {
                                FailoverBehavior = FailoverBehavior.ReadFromLeaderWriteToLeader
                            },
                            FailoverServers = new Abstractions.Data.FailoverServers
                                              {
                                                  ForDefaultDatabase = new[]
                                                                       {
                                                                           new ReplicationDestination
                                                                           {
                                                                               Url = clusterStores[0].Url,
                                                                               Database = clusterStores[0].DefaultDatabase
                                                                           }
                                                                       }
                                              }
                        })
            {
                store.Initialize();

                store.DatabaseCommands.Put("key/1", null, new RavenJObject(), new RavenJObject());
            }

            clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "key/1"));
        }
    }
}
