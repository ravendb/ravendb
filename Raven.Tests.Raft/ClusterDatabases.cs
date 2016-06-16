// -----------------------------------------------------------------------
//  <copyright file="ClusterDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

using Rachis.Transport;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Database.Raft.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Raft
{
    public class ClusterDatabases : RaftTestBase
    {
        [Fact]
        public async Task CanCreateClusterWhenThereAreNoDatabasesOnServer()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var databases = store.DatabaseCommands.ForSystemDatabase().StartsWith(Constants.Database.Prefix, null, 0, 1024);
                foreach (var database in databases)
                    store.DatabaseCommands.GlobalAdmin.DeleteDatabase(database.Key.Substring(Constants.Database.Prefix.Length));

                var request = store.DatabaseCommands.ForSystemDatabase().CreateRequest("/admin/cluster/create", HttpMethod.Post);
                await request.WriteAsync(RavenJObject.FromObject(new NodeConnectionInfo()));
            }
        }

        [Fact]
        public async Task CannotCreateClusterWhenThereAreAnyDatabasesOnServer()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var request = store.DatabaseCommands.ForSystemDatabase().CreateRequest("/admin/cluster/create", HttpMethod.Post);
                var e = await AssertAsync.Throws<ErrorResponseException>(() => request.WriteAsync(RavenJObject.FromObject(new NodeConnectionInfo())));

                Assert.Equal("To create a cluster server must not contain any databases.", e.Message);
            }
        }

        [Fact]
        public async Task CannotJoinNodeWithExistingDatabases()
        {
            using (var storeToJoin = NewRemoteDocumentStore())
            {
                storeToJoin.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("testDb");

                var request = storeToJoin.DatabaseCommands.ForSystemDatabase().CreateRequest("/admin/cluster/canJoin?topologyId=" + Guid.NewGuid(), HttpMethod.Get);
                var response = await request.ExecuteRawResponseAsync();

                // since we have testDb on storeToJoin we answer with can't join 
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            }
        }

        [Theory]
        [PropertyData("Nodes")]
        public void DatabaseShouldBeCreatedOnAllNodes(int numberOfNodes)
        {
            var clusterStores = CreateRaftCluster(numberOfNodes);

            using (var store1 = clusterStores[0])
            {
                store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                                                                   {
                                                                       Id = "Northwind",
                                                                       Settings =
                                                                       {
                                                                           {"Raven/DataDir", "~/Databases/Northwind"}
                                                                       }
                                                                   });

                var key = Constants.Database.Prefix + "Northwind";

                clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), key));
            }
        }

        [Theory]
        [PropertyData("Nodes")]
        public void CanWaitUntilDatabaseIsCreatedOnCallingNode(int numberOfNodes)
        {
            var clusterStores = CreateRaftCluster(numberOfNodes);

            var firstNonLeaderIndex = servers.FindIndex(server => !server.Options.ClusterManager.Value.IsLeader());

            using (var nonLeaderStore = clusterStores[firstNonLeaderIndex])
            {
                nonLeaderStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "Northwind",
                    Settings =
                                                                       {
                                                                           {"Raven/DataDir", "~/Databases/Northwind"}
                                                                       }
                });

                // if create database waits properly until database is being created on calling node
                // then we can send request to newly created database (and won't get Could not find a resource named: Northwind exception)
                Assert.Null(nonLeaderStore.DatabaseCommands.ForDatabase("Northwind").Get("people/1"));
            }
        }

        [Theory]
        [PropertyData("Nodes")]
        public async Task CanUpdateDatabaseOnAllNodes(int numberOfNodes)
        {
            var clusterStores = CreateRaftCluster(numberOfNodes);

            using (var store1 = clusterStores[0])
            {
                // arrange
                var requestCreator = new AdminRequestCreator((url, method) => store1.DatabaseCommands.ForSystemDatabase().CreateRequest(url, method), null);

                store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "Northwind",
                    Settings =
                                    {
                                        { "Raven/DataDir", "~/Databases/Northwind" },
                                        { "Raven/ActiveBundles", "Replication"}
                                    }
                });

                var key = Constants.Database.Prefix + "Northwind";

                clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), key));

                // act - try to add new setting
                var databaseUpdateDocument = new DatabaseDocument
                {
                    Id = "Northwind",
                    Settings =
                    {
                        {"Raven/DataDir", "~/Databases/Northwind"},
                        {"Raven/ActiveBundles", "Replication"},
                        {"Raven/New", "testing" }
                    }
                };

                var existingDbEtag = store1.DatabaseCommands.ForSystemDatabase().Get(key).Etag;

                RavenJObject doc;
                using (var req = requestCreator.CreateDatabase(databaseUpdateDocument, out doc))
                {
                    req.AddHeader("If-None-Match", existingDbEtag);
                    await req.WriteAsync(doc.ToString(Formatting.Indented)).ConfigureAwait(false);
                }

                // assert
                clusterStores.ForEach(store =>
                {
                    WaitFor(store.DatabaseCommands, commands =>
                    {
                        var databaseDocument = commands.ForSystemDatabase().Get(key);
                        var settings = databaseDocument.DataAsJson.Value<RavenJObject>("Settings");
                        return "testing" == settings.Value<string>("Raven/New");
                    });
                });
            }
        }

        [Theory]
        [PropertyData("Nodes")]
        public void DatabaseShouldBeDeletedOnAllNodes(int numberOfNodes)
        {
            var clusterStores = CreateRaftCluster(numberOfNodes);

            using (var store1 = clusterStores[0])
            {
                store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "Northwind",
                    Settings =
                    {
                        {"Raven/DataDir", "~/Databases/Northwind"},
                        {Constants.Cluster.NonClusterDatabaseMarker, "false"}
                    }
                });

                var key = Constants.Database.Prefix + "Northwind";

                clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), key));

                store1.DatabaseCommands.GlobalAdmin.DeleteDatabase(key);

                clusterStores.ForEach(store => WaitForDelete(store.DatabaseCommands.ForSystemDatabase(), key));
            }
        }

        [Fact]
        public void NonClusterDatabasesShouldNotBeCreatedOnAllNodes()
        {
            var clusterStores = CreateRaftCluster(3);

            using (var store1 = clusterStores[0])
            using (var store2 = clusterStores[1])
            using (var store3 = clusterStores[2])
            {
                store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "Northwind",
                    Settings =
                    {
                        {"Raven/DataDir", "~/Databases/Northwind"},
                        {Constants.Cluster.NonClusterDatabaseMarker, "true"}
                    }
                });

                var key = Constants.Database.Prefix + "Northwind";

                Assert.NotNull(store1.DatabaseCommands.ForSystemDatabase().Get(key));

                var e = Assert.Throws<Exception>(() => WaitForDocument(store2.DatabaseCommands.ForSystemDatabase(), key, TimeSpan.FromSeconds(10)));
                Assert.Equal("WaitForDocument failed", e.Message);

                e = Assert.Throws<Exception>(() => WaitForDocument(store3.DatabaseCommands.ForSystemDatabase(), key, TimeSpan.FromSeconds(10)));
                Assert.Equal("WaitForDocument failed", e.Message);
            }
        }
    }
}
