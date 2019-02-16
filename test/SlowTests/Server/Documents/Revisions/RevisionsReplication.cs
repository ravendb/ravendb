//-----------------------------------------------------------------------
// <copyright file="Revisions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Revisions;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Sdk;

namespace SlowTests.Server.Documents.Revisions
{
    public class RevisionsReplication : ReplicationTestBase, ITombstoneAware
    {
        private void WaitForMarker(DocumentStore store1, DocumentStore store2)
        {
            var id = "marker - " + Guid.NewGuid();
            using (var session = store1.OpenSession())
            {
                session.Store(new Product { Name = "Marker" }, id);
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(store2, id));
        }

        [Fact]
        public async Task CanGetAllRevisionsFor()
        {
            var company = new Company { Name = "Company Name" };
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database);
                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                using (var session = store1.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    company3.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }
                WaitForMarker(store1, store2);
                using (var session = store2.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(2, companiesRevisions.Count);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[0].Name);
                    Assert.Equal("Company Name", companiesRevisions[1].Name);
                }
            }
        }

        [Fact]
        public async Task CanCheckIfDocumentHasRevisions()
        {
            var company = new Company { Name = "Company Name" };
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database);
                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                WaitForMarker(store1, store2);
                using (var session = store2.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    var metadata = session.Advanced.GetMetadataFor(company3);

                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.FromReplication).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                }
            }
        }

        [Fact]
        public async Task WillDeleteOldRevisions()
        {
            var company = new Company { Name = "Company #1" };
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database);
                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                    for (var i = 0; i < 10; i++)
                    {
                        company.Name = "Company #2: " + i;
                        await session.SaveChangesAsync();
                    }
                }

                WaitForMarker(store1, store2);
                await AssertRevisions(store1, company, "1");
                await AssertRevisions(store2, company, "2");
            }
        }

        private async Task AssertRevisions(DocumentStore store, Company company, string tag)
        {
            using (var session = store.OpenAsyncSession())
            {
                var revisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                try
                {
                    Assert.Equal(5, revisions.Count);
                    Assert.Equal("Company #2: 9", revisions[0].Name);
                    Assert.Equal("Company #2: 5", revisions[4].Name);
                }
                catch (Exception e)
                {
                    throw new XunitException($"Tag: {tag}. " +
                                             $"Revisions: {string.Join(", ", revisions.Select(r => r.Name))}. " +
                                             $"Exception: {e}");
                }
            }
        }

        [Fact]
        public async Task RevisionsOrder()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database);
                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Hibernating" }, "users/1");
                    await session.StoreAsync(new User { Name = "Hibernating11" }, "users/11");
                    await session.SaveChangesAsync();
                }
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Hibernating Rhinos" }, "users/1");
                    await session.StoreAsync(new User { Name = "Hibernating Rhinos11" }, "users/11");
                    await session.SaveChangesAsync();
                }
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Hibernating Rhinos - RavenDB" }, "users/1");
                    await session.StoreAsync(new User { Name = "Hibernating Rhinos - RavenDB11" }, "users/11");
                    await session.SaveChangesAsync();
                }

                WaitForMarker(store1, store2);
                using (var session = store2.OpenAsyncSession())
                {
                    var users = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(3, users.Count);
                    Assert.Equal("Hibernating Rhinos - RavenDB", users[0].Name);
                    Assert.Equal("Hibernating Rhinos", users[1].Name);
                    Assert.Equal("Hibernating", users[2].Name);
                }
            }
        }

        [Fact]
        public async Task ShouldRevisionOnReplication()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database);
                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Hibernating" }, "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument(store2, "users/1"));

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.Advanced.Revisions.GetMetadataForAsync("users/1");
                    Assert.NotNull(user);
                    Assert.Equal(1, user.Count);
                }
            }
        }

        [Fact]
        public async Task ConflictedRevisionShouldReplicateBack()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await PutDocument(store1);
                await PutDocument(store2);

                await SetupReplicationAsync(store1, store2);
                WaitForMarker(store1, store2);

                await PutDocument(store1);
                WaitForMarker(store1, store2);

                await SetupReplicationAsync(store2, store1);
                WaitForMarker(store2, store1);

                using (var session1 = store1.OpenAsyncSession())
                using (var session2 = store2.OpenAsyncSession())
                {
                    var doc1 = await session1.LoadAsync<User>("foo/bar");
                    var doc2 = await session2.LoadAsync<User>("foo/bar");

                    Assert.Equal(doc1.Name, doc2.Name);

                    var rev1 = await session1.Advanced.Revisions.GetMetadataForAsync("foo/bar", pageSize: int.MaxValue);
                    var rev2 = await session2.Advanced.Revisions.GetMetadataForAsync("foo/bar", pageSize: int.MaxValue);

                    Assert.Equal(rev1.Count, rev2.Count);
                }
            }
        }

        [Fact]
        public async Task ResolvedRevisionShouldReplicateBack()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await PutDocument(store1);
                await PutDocument(store2);

                await SetupReplicationAsync(store1, store2);
                WaitForMarker(store1, store2);

                await SetupReplicationAsync(store2, store1);
                WaitForMarker(store2, store1);

                using (var session1 = store1.OpenAsyncSession())
                using (var session2 = store2.OpenAsyncSession())
                {
                    var doc1 = await session1.LoadAsync<User>("foo/bar");
                    var doc2 = await session2.LoadAsync<User>("foo/bar");

                    Assert.Equal(doc1.Name, doc2.Name);

                    var rev1 = await session1.Advanced.Revisions.GetMetadataForAsync("foo/bar", pageSize: int.MaxValue);
                    var rev2 = await session2.Advanced.Revisions.GetMetadataForAsync("foo/bar", pageSize: int.MaxValue);

                    Assert.Equal(rev1.Count, rev2.Count);
                }
            }
        }

        [Fact]
        public async Task IdenticalRevisionCountCluster()
        {
            var cluster = await CreateRaftCluster(3);
            var database = GetDatabaseName();

            using (var store1 = GetDocumentStore(new Options
            {
                Server = cluster.Nodes[0],
                CreateDatabase = false,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                },
                ModifyDatabaseName = _ => database
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                Server = cluster.Nodes[1],
                CreateDatabase = false,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                },
                ModifyDatabaseName = _ => database
            }))
            {
                store1.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)
                {
                    Topology = new DatabaseTopology
                    {
                        Members = new List<string>
                        {
                            cluster.Nodes[0].ServerStore.NodeTag,
                            cluster.Nodes[1].ServerStore.NodeTag
                        }
                    }
                }, 2));

                await Task.WhenAll(PutMultiDocuments(store1), PutMultiDocuments(store2));

                WaitForMarker(store1, store2);
                WaitForMarker(store2, store1);

                using (var session1 = store1.OpenAsyncSession())
                using (var session2 = store2.OpenAsyncSession())
                {
                    var doc1 = await session1.LoadAsync<User>("foo/bar");
                    var doc2 = await session2.LoadAsync<User>("foo/bar");

                    Assert.Equal(doc1.Name, doc2.Name);

                    var rev1 = await session1.Advanced.Revisions.GetMetadataForAsync("foo/bar", pageSize: int.MaxValue);
                    var rev2 = await session2.Advanced.Revisions.GetMetadataForAsync("foo/bar", pageSize: int.MaxValue);

                    Assert.Equal(rev1.Count, rev2.Count);
                }
            }
        }

        [Fact]
        public async Task IdenticalRevisionCountExternal()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                await Task.WhenAll(PutMultiDocuments(store1), PutMultiDocuments(store2));

                WaitForMarker(store1, store2);
                WaitForMarker(store2, store1);

                using (var session1 = store1.OpenAsyncSession())
                using (var session2 = store2.OpenAsyncSession())
                {
                    var doc1 = await session1.LoadAsync<User>("foo/bar");
                    var doc2 = await session2.LoadAsync<User>("foo/bar");

                    Assert.Equal(doc1.Name, doc2.Name);

                    var rev1 = await session1.Advanced.Revisions.GetMetadataForAsync("foo/bar", pageSize: int.MaxValue);
                    var rev2 = await session2.Advanced.Revisions.GetMetadataForAsync("foo/bar", pageSize: int.MaxValue);

                    Assert.Equal(rev1.Count, rev2.Count);
                }
            }
        }

        private static async Task PutDocument(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = RandomString(5) };
                await session.StoreAsync(user, "foo/bar");
                await session.SaveChangesAsync();
            }
        }

        private async Task PutMultiDocuments(IDocumentStore store)
        {
            for (var i = 0; i < 10; i++)
            {
                try
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var loaded = await session.LoadAsync<User>("foo/bar");
                        if (loaded == null)
                        {
                            loaded = new User();
                            await session.StoreAsync(loaded, "foo/bar");
                        }

                        loaded.Name = RandomString(5);
                        
                        await session.StoreAsync(loaded, "foo/bar");
                        await session.SaveChangesAsync();
                    }
                }
                catch
                {
                    //
                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task GetRevisionsBinEntries(bool useSession)
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store1);
                var database2 = await GetDocumentDatabaseInstanceFor(store2);
                database.TombstoneCleaner.Subscribe(this);
                database2.TombstoneCleaner.Subscribe(this);

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database, configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = false;
                });
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database, configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = false;
                });
                await SetupReplicationAsync(store1, store2);

                var deletedRevisions = await store1.Commands().GetRevisionsBinEntriesAsync(long.MaxValue);
                Assert.Equal(0, deletedRevisions.Length);

                var id = "users/1";
                if (useSession)
                {
                    var user = new User { Name = "Fitzchak" };
                    for (var i = 0; i < 2; i++)
                    {
                        using (var session = store1.OpenAsyncSession())
                        {
                            await session.StoreAsync(user);
                            await session.SaveChangesAsync();
                        }
                        using (var session = store1.OpenAsyncSession())
                        {
                            session.Delete(user.Id);
                            await session.SaveChangesAsync();
                        }
                    }
                    id += "-A";
                }
                else
                {
                    await store1.Commands().PutAsync(id, null, new User { Name = "Fitzchak" });
                    await store1.Commands().DeleteAsync(id, null);
                    await store1.Commands().PutAsync(id, null, new User { Name = "Fitzchak" });
                    await store1.Commands().DeleteAsync(id, null);
                }

                WaitForMarker(store1, store2);
                var statistics = store2.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(useSession ? 2 : 1, statistics.CountOfDocuments);
                Assert.Equal(4, statistics.CountOfRevisionDocuments);

                //sanity
                deletedRevisions = await store1.Commands().GetRevisionsBinEntriesAsync(long.MaxValue);
                Assert.Equal(1, deletedRevisions.Length);

                deletedRevisions = await store2.Commands().GetRevisionsBinEntriesAsync(long.MaxValue);
                Assert.Equal(1, deletedRevisions.Length);

                using (var session = store2.OpenAsyncSession())
                {
                    var users = await session.Advanced.Revisions.GetForAsync<User>(id);
                    Assert.Equal(4, users.Count);
                    Assert.Equal(null, users[0].Name);
                    Assert.Equal("Fitzchak", users[1].Name);
                    Assert.Equal(null, users[2].Name);
                    Assert.Equal("Fitzchak", users[3].Name);

                    // Can get metadata only
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(4, revisionsMetadata.Count);
                    Assert.Equal((DocumentFlags.DeleteRevision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication).ToString(), revisionsMetadata[0].GetString(Constants.Documents.Metadata.Flags));
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.FromReplication).ToString(), revisionsMetadata[1].GetString(Constants.Documents.Metadata.Flags));
                    Assert.Equal((DocumentFlags.DeleteRevision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication).ToString(), revisionsMetadata[2].GetString(Constants.Documents.Metadata.Flags));
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.FromReplication).ToString(), revisionsMetadata[3].GetString(Constants.Documents.Metadata.Flags));
                }

                await store1.Maintenance.SendAsync(new RevisionsTests.DeleteRevisionsOperation(new AdminRevisionsHandler.Parameters
                {
                    DocumentIds = new[] { id, "users/not/exists" }
                }));
                WaitForMarker(store1, store2);

                statistics = store2.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(useSession ? 3 : 2, statistics.CountOfDocuments);

                Assert.Equal(0, statistics.CountOfRevisionDocuments);
            }
        }

        private static readonly Random Random = new Random(357);
        public static string RandomString(int length)
        {
            const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";
            var str = new char[length];
            for (int i = 0; i < length; i++)
            {
                str[i] = chars[Random.Next(chars.Length)];
            }
            return new string(str);
        }

        public Task ReplicateExpiredAndDeletedRevisions(/*bool useSession*/)
        {
            return Task.CompletedTask;
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Product
        {
            public string Name { get; set; }
        }

        public string TombstoneCleanerIdentifier => nameof(RevisionsReplication);

        public Dictionary<string, long> GetLastProcessedTombstonesPerCollection()
        {
            return new Dictionary<string, long>
            {
                ["Products"] = 0,
                ["Users"] = 0
            };
        }
    }
}
