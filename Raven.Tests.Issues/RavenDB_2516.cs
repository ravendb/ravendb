// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2516.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Database.Bundles.Replication.Data;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2516 : ReplicationBase
    {
        protected override void ModifyConfiguration(InMemoryRavenConfiguration serverConfiguration)
        {
            Authentication.EnableOnce();
        }

        [Fact]
        public void ReplicationTopologyDiscovererSimpleTest()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            using (var store3 = CreateStore())
            using (var store4 = CreateStore())
            using (var store5 = CreateStore())
            {
                using (var session1 = store1.OpenSession())
                {
                    session1.Store(new Person { Name = "Name1" });
                    session1.SaveChanges();
                }

                RunReplication(store1, store2, TransitiveReplicationOptions.Replicate);
                RunReplication(store2, store3, TransitiveReplicationOptions.Replicate);
                RunReplication(store3, store4, TransitiveReplicationOptions.Replicate);
                RunReplication(store4, store5, TransitiveReplicationOptions.Replicate);
                RunReplication(store5, store1, TransitiveReplicationOptions.Replicate);

                WaitForDocument<Person>(store5, "people/1");

                var url = store1.Url.ForDatabase(store1.DefaultDatabase) + "/admin/replication/topology/view";

                var request = store1
                    .JsonRequestFactory
                    .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Post, store1.DatabaseCommands.PrimaryCredentials, store1.Conventions));

                var json = (RavenJObject)request.ReadResponseJson();
                var topology = json.Deserialize<ReplicationTopology>(store1.Conventions);

                Assert.NotNull(topology);
                Assert.Equal(5, topology.Servers.Count);
                Assert.Equal(5, topology.Connections.Count);

                topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store1.Url.ForDatabase(store1.DefaultDatabase))
                                                 && x.SourceUrl.Any(y => y == store5.Url.ForDatabase(store5.DefaultDatabase)));
                topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store2.Url.ForDatabase(store2.DefaultDatabase))
                                                 && x.SourceUrl.Any(y => y == store1.Url.ForDatabase(store1.DefaultDatabase)));
                topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store3.Url.ForDatabase(store3.DefaultDatabase))
                                                 && x.SourceUrl.Any(y => y == store2.Url.ForDatabase(store2.DefaultDatabase)));
                topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store4.Url.ForDatabase(store4.DefaultDatabase))
                                                 && x.SourceUrl.Any(y => y == store3.Url.ForDatabase(store3.DefaultDatabase)));
                topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store5.Url.ForDatabase(store5.DefaultDatabase))
                                                 && x.SourceUrl.Any(y => y == store4.Url.ForDatabase(store4.DefaultDatabase)));

                foreach (var connection in topology.Connections.Where(x => x.DestinationUrl
                    .All(y => y != store1.Url.ForDatabase(store1.DefaultDatabase))
                            && x.SourceUrl.All(y => y != store5.Url.ForDatabase(store5.DefaultDatabase))))
                {
                    Assert.Equal(ReplicatonNodeState.Online, connection.SourceToDestinationState);
                    Assert.Equal(ReplicatonNodeState.Online, connection.DestinationToSourceState);
                    Assert.NotNull(connection.Source);
                    Assert.NotNull(connection.Destination);
                    Assert.Equal(TransitiveReplicationOptions.Replicate, connection.ReplicationBehavior);
                    Assert.NotNull(connection.LastAttachmentEtag);
                    Assert.NotNull(connection.LastDocumentEtag);
                    Assert.NotNull(connection.SendServerId);
                    Assert.NotNull(connection.StoredServerId);
                }

                var c = topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store1.Url.ForDatabase(store1.DefaultDatabase))
                                                         && x.SourceUrl.Any(y => y == store5.Url.ForDatabase(store5.DefaultDatabase)));
                Assert.Equal(ReplicatonNodeState.Online, c.SourceToDestinationState);
                Assert.Equal(ReplicatonNodeState.Offline, c.DestinationToSourceState);
                Assert.NotNull(c.Source);
                Assert.NotNull(c.Destination);
                Assert.Equal(TransitiveReplicationOptions.Replicate, c.ReplicationBehavior);
                Assert.Null(c.LastAttachmentEtag);
                Assert.Null(c.LastDocumentEtag);
                Assert.NotNull(c.SendServerId);
                Assert.Equal(Guid.Empty, c.StoredServerId);
            }
        }

        [Fact]
        public void ReplicationTopologyDiscovererSimpleTestWithOAuth()
        {
            using (var store1 = CreateStore(enableAuthorization: true, anonymousUserAccessMode: AnonymousUserAccessMode.None, configureStore: store => store.ApiKey = "Ayende/abc"))
            using (var store2 = CreateStore(enableAuthorization: true, anonymousUserAccessMode: AnonymousUserAccessMode.None, configureStore: store => store.ApiKey = "Ayende/abc"))
            using (var store3 = CreateStore(enableAuthorization: true, anonymousUserAccessMode: AnonymousUserAccessMode.None, configureStore: store => store.ApiKey = "Ayende/abc"))
            using (var store4 = CreateStore(enableAuthorization: true, anonymousUserAccessMode: AnonymousUserAccessMode.None, configureStore: store => store.ApiKey = "Ayende/abc"))
            using (var store5 = CreateStore(enableAuthorization: true, anonymousUserAccessMode: AnonymousUserAccessMode.None, configureStore: store => store.ApiKey = "Ayende/abc"))
            {
                foreach (var server in servers)
                {
                    server.SystemDatabase.Documents.Put("Raven/ApiKeys/Ayende", null, RavenJObject.FromObject(new ApiKeyDefinition
                    {
                        Databases = new List<ResourceAccess>
                                    {
                                        new ResourceAccess { TenantId = "*", Admin = true }, 
                                        new ResourceAccess { TenantId = "<system>", Admin = true },
                                    },
                        Enabled = true,
                        Name = "Ayende",
                        Secret = "abc"
                    }), new RavenJObject(), null);
                }

                using (var session1 = store1.OpenSession())
                {
                    session1.Store(new Person { Name = "Name1" });
                    session1.SaveChanges();
                }

                RunReplication(store1, store2, TransitiveReplicationOptions.Replicate, apiKey: "Ayende/abc");
                RunReplication(store2, store3, TransitiveReplicationOptions.Replicate, apiKey: "Ayende/abc");
                RunReplication(store3, store4, TransitiveReplicationOptions.Replicate, apiKey: "Ayende/abc");
                RunReplication(store4, store5, TransitiveReplicationOptions.Replicate, apiKey: "Ayende/abc");
                RunReplication(store5, store1, TransitiveReplicationOptions.Replicate, apiKey: "Ayende/abc");

                WaitForDocument<Person>(store5, "people/1");

                var url = store1.Url.ForDatabase(store1.DefaultDatabase) + "/admin/replication/topology/view";

                var request = store1
                    .JsonRequestFactory
                    .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Post, store1.DatabaseCommands.PrimaryCredentials, store1.Conventions));

                var json = (RavenJObject)request.ReadResponseJson();
                var topology = json.Deserialize<ReplicationTopology>(store1.Conventions);

                Assert.NotNull(topology);
                Assert.Equal(5, topology.Servers.Count);
                Assert.Equal(5, topology.Connections.Count);

                topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store1.Url.ForDatabase(store1.DefaultDatabase))
                                                 && x.SourceUrl.Any(y => y == store5.Url.ForDatabase(store5.DefaultDatabase)));
                topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store2.Url.ForDatabase(store2.DefaultDatabase))
                                                 && x.SourceUrl.Any(y => y == store1.Url.ForDatabase(store1.DefaultDatabase)));
                topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store3.Url.ForDatabase(store3.DefaultDatabase))
                                                 && x.SourceUrl.Any(y => y == store2.Url.ForDatabase(store2.DefaultDatabase)));
                topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store4.Url.ForDatabase(store4.DefaultDatabase))
                                                 && x.SourceUrl.Any(y => y == store3.Url.ForDatabase(store3.DefaultDatabase)));
                topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store5.Url.ForDatabase(store5.DefaultDatabase))
                                                 && x.SourceUrl.Any(y => y == store4.Url.ForDatabase(store4.DefaultDatabase)));

                foreach (var connection in topology.Connections
                    .Where(x => x.DestinationUrl.All(y => y != store1.Url.ForDatabase(store1.DefaultDatabase))
                                && x.SourceUrl.All(y => y != store5.Url.ForDatabase(store5.DefaultDatabase))))
                {
                    Assert.Equal(ReplicatonNodeState.Online, connection.SourceToDestinationState);
                    Assert.Equal(ReplicatonNodeState.Online, connection.DestinationToSourceState);
                    Assert.NotNull(connection.Source);
                    Assert.NotNull(connection.Destination);
                    Assert.Equal(TransitiveReplicationOptions.Replicate, connection.ReplicationBehavior);
                    Assert.NotNull(connection.LastAttachmentEtag);
                    Assert.NotNull(connection.LastDocumentEtag);
                    Assert.NotNull(connection.SendServerId);
                    Assert.NotNull(connection.StoredServerId);
                }

                var c = topology.Connections.Single(x => x.DestinationUrl.Any(y => y == store1.Url.ForDatabase(store1.DefaultDatabase))
                                                         && x.SourceUrl.Any(y => y == store5.Url.ForDatabase(store5.DefaultDatabase)));
                Assert.Equal(ReplicatonNodeState.Online, c.SourceToDestinationState);
                Assert.Equal(ReplicatonNodeState.Offline, c.DestinationToSourceState);
                Assert.NotNull(c.Source);
                Assert.NotNull(c.Destination);
                Assert.Equal(TransitiveReplicationOptions.Replicate, c.ReplicationBehavior);
                Assert.Null(c.LastAttachmentEtag);
                Assert.Null(c.LastDocumentEtag);
                Assert.NotNull(c.SendServerId);
                Assert.Equal(Guid.Empty, c.StoredServerId);
            }
        }
    }
}
