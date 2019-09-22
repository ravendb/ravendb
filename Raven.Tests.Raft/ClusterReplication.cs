// -----------------------------------------------------------------------
//  <copyright file="ClusterReplication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Database.Raft.Dto;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Raft
{
    public class ClusterReplication : RaftTestBase
    {
        [Fact]
        public void EnablingReplicationInClusterWillCreateGlobalReplicationDestinationsOnEachNode()
        {
            var clusterStores = CreateRaftCluster(3);

            using (clusterStores[0])
            using (clusterStores[1])
            using (clusterStores[2])
            {
                SetupClusterConfiguration(clusterStores);

                AssertReplicationDestinations(clusterStores, (i, j, destination) =>
                {
                    Assert.False(destination.Disabled);
                    Assert.Null(destination.Database);
                    Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
                });

                clusterStores.ForEach(store => store.DatabaseCommands.ForSystemDatabase().Delete(Constants.Global.ReplicationDestinationsDocumentName, null));

                SetupClusterConfiguration(clusterStores, false);

                clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));

                AssertReplicationDestinations(clusterStores, (i, j, destination) =>
                {
                    Assert.True(destination.Disabled);
                    Assert.Null(destination.Database);
                    Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
                });
            }
        }

        public void AssertReplicationDestinations(List<DocumentStore> stores, Action<int, int, ReplicationDestination> extraChecks = null)
        {
            for (var i = 0; i < stores.Count; i++)
            {
                var destinationsJson = stores[i].DatabaseCommands.ForSystemDatabase().Get(Constants.Global.ReplicationDestinationsDocumentName);
                var destinations = destinationsJson.DataAsJson.JsonDeserialization<ReplicationDocument>();
                Assert.Equal(stores.Count - 1, destinations.Destinations.Count);
                for (var j = 0; j < stores.Count; j++)
                {
                    if (j == i)
                        continue;
                    var destination = destinations.Destinations.First(x => string.Equals(x.Url, stores[j].Url));
                    if (extraChecks != null)
                    {
                        extraChecks(i, j, destination);
                    }
                }
            }
        }

        [Fact]
        public async Task WhenChangingTopologyReplicationShouldBeConfiguredProperly()
        {
            var clusterStores = CreateRaftCluster(3);
            var topologyId = servers.First().Options.ClusterManager.Value.Engine.CurrentTopology.TopologyId;
            using (clusterStores[0])
            using (clusterStores[1])
            using (clusterStores[2])
            {
                var client = servers[0].Options.ClusterManager.Value.Client;
                await client.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = true });

                clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));

                AssertReplicationDestinations(clusterStores, (i, j, destination) =>
                {
                    Assert.False(destination.Disabled);
                    Assert.Null(destination.Database);
                    Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
                });

                var extraStores = ExtendRaftCluster(2, topologyId);
                using (extraStores[0])
                using (extraStores[1])
                {
                    extraStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));
                    AssertReplicationDestinations(clusterStores.Concat(extraStores).ToList(), (i, j, destination) =>
                    {
                        Assert.False(destination.Disabled);
                        Assert.Null(destination.Database);
                        Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
                    });

                    var allStores = clusterStores.Concat(extraStores).ToList();
                    allStores.RemoveAt(4);

                    // fetch etags of each replication destination document
                    var etags = allStores.Select(store => store.DatabaseCommands.ForSystemDatabase().Head(Constants.Global.ReplicationDestinationsDocumentName).Etag).ToList();

                    RemoveFromCluster(servers[4], topologyId);

                    for (var i = 0; i < allStores.Count; i++)
                    {
                        WaitForDocument(allStores[i].DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName, etags[i]);
                    }

                    AssertReplicationDestinations(allStores, (i, j, destination) =>
                    {
                        Assert.False(destination.Disabled);
                        Assert.Null(destination.Database);
                        Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
                    });
                }
            }
        }


        [Fact]
        public void InterClusterDocumentsETL()
        {
            var cluster1Stores = CreateRaftCluster(3, activeBundles: "Replication");
            var cluster2Stores = CreateRaftCluster(3, activeBundles: "Replication");

            SetupClusterConfiguration(cluster1Stores);
            SetupClusterConfiguration(cluster2Stores);
            cluster1Stores.First().DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                null,
                new RavenJObject() {
                    {
                        "Destinations", new RavenJArray(new RavenJObject {
                            { "Url", cluster2Stores.First().Url},
                            { "Database", cluster2Stores.First().DefaultDatabase },
                            { "SpecifiedCollections", RavenJObject.FromObject(new Dictionary<string, string>
                                {
                                    { "users", null }
                                }) }

                        } )
                    }
                }
                , new RavenJObject()

                );
            cluster1Stores.First().DatabaseCommands.Put("users/1", null, new RavenJObject() { { "Foo", "Bar" } }, new RavenJObject() { { "Raven-Entity-Name", "users" } });

            RavenJObject document = null;

            for (int i = 0; i < 100 * 10; i++)
            {
                using (var session = cluster2Stores.First().OpenSession())
                {
                    document = session.Load<RavenJObject>("users/1");
                    if (document != null)
                        break;
                    Thread.Sleep(100);
                }
            }

            Assert.NotNull(document);

        }

        [Fact]
        public void InterClusterAttachmentsETL()
        {
            var cluster1Stores = CreateRaftCluster(3, activeBundles: "Replication");
            var cluster2Stores = CreateRaftCluster(3, activeBundles: "Replication");

            SetupClusterConfiguration(cluster1Stores);
            SetupClusterConfiguration(cluster2Stores);
            cluster1Stores.First().DatabaseCommands.Put(Constants.RavenReplicationDestinations,
                null,
                new RavenJObject() {
                    {
                        "Destinations", new RavenJArray(new RavenJObject {
                            { "Url", cluster2Stores.First().Url},
                            { "Database", cluster2Stores.First().DefaultDatabase },
                            { "SpecifiedCollections", RavenJObject.FromObject(new Dictionary<string, string>
                                {
                                    { "users", null }
                                }) },
                            {"ReplicateAttachmentsInEtl", "true" }

                        } )
                    }
                }
                , new RavenJObject()

                );

            cluster1Stores.First().DatabaseCommands.PutAttachment("attach/1", null, new System.IO.MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());



            Attachment attachment = null;

            for (int i = 0; i < 100 * 10; i++)
            {
                attachment = cluster2Stores.First().DatabaseCommands.GetAttachment("attach/1");
                if (attachment != null)
                    break;
                Thread.Sleep(100);
            }

            Assert.NotNull(attachment);

        }
    }
}
