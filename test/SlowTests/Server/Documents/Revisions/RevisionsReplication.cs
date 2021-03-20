//-----------------------------------------------------------------------
// <copyright file="Revisions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Revisions;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Sdk;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Revisions
{
    public class RevisionsReplication : ReplicationTestBase, ITombstoneAware
    {
        public RevisionsReplication(ITestOutputHelper output) : base(output)
        {
        }

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
        public async Task ConflictedRevisionClearedAfterEnforce()
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

                var db = await GetDocumentDatabaseInstanceFor(store1);

                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                    await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);

                WaitForMarker(store1, store2);

                using (var session1 = store1.OpenAsyncSession())
                using (var session2 = store2.OpenAsyncSession())
                {
                    var doc1 = await session1.LoadAsync<User>("foo/bar");
                    var doc2 = await session2.LoadAsync<User>("foo/bar");

                    Assert.Equal(doc1.Name, doc2.Name);

                    var rev1 = await session1.Advanced.Revisions.GetMetadataForAsync("foo/bar", pageSize: int.MaxValue);
                    var rev2 = await session2.Advanced.Revisions.GetMetadataForAsync("foo/bar", pageSize: int.MaxValue);

                    Assert.Equal(0, rev1.Count);
                    Assert.Equal(0, rev2.Count);
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
                Assert.Equal(0, deletedRevisions.Count());

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
                Assert.Equal(1, deletedRevisions.Count());

                deletedRevisions = await store2.Commands().GetRevisionsBinEntriesAsync(long.MaxValue);
                Assert.Equal(1, deletedRevisions.Count());

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

        [Fact]
        public async Task ReplicateExpiredRevisions()
        {
            var revisionsAgeLimit = TimeSpan.FromSeconds(10);

            Action<RevisionsConfiguration> modifyConfiguration = configuration =>
                configuration.Collections["Users"] = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionAgeToKeep = revisionsAgeLimit
                };

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database, modifyConfiguration);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database, modifyConfiguration);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");

                    await session.SaveChangesAsync();
                }

                for (int i = 2; i <= 10; i++)
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>("users/1-A");
                        user.Name = "Aviv" + i;
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>("users/1-A");
                    Assert.Equal(10, revisions.Count);
                }


                // wait until revisions are expired
                await Task.Delay(revisionsAgeLimit);

                // setup replication 
                await SetupReplicationAsync(store1, store2);
                WaitForMarker(store1, store2);

                // store1 should still have 10 revisions
                // store2 should have no revisions 
                using (var session = store1.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>("users/1-A");
                    Assert.Equal(10, revisions.Count);
                }
                using (var session = store2.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>("users/1-A");
                    Assert.Equal(0, revisions.Count);
                }

                /*
                                // TODO : RavenDB-13359
                                using (var session = store2.OpenAsyncSession())
                                {
                                    var doc = await session.LoadAsync<User>("users/1-A");
                                    var md = session.Advanced.GetMetadataFor(doc);
                                    md.TryGetValue(Constants.Documents.Metadata.Flags, out var flags);

                                    Assert.DoesNotContain(nameof(DocumentFlags.HasRevisions), flags);
                                }
                */

                // modify doc on store1 to create another revision
                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1-A");
                    user.Name = "Grisha";
                    await session.SaveChangesAsync();
                }

                WaitForMarker(store1, store2);

                // assert that both stores have just one revision
                using (var session = store1.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>("users/1-A");
                    Assert.Equal(1, revisions.Count);
                    Assert.Equal("Grisha", revisions[0].Name);
                }
                using (var session = store2.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>("users/1-A");
                    Assert.Equal(1, revisions.Count);
                    Assert.Equal("Grisha", revisions[0].Name);
                }

            }

        }

        [Fact]
        public async Task ReplicateExpiredAndDeletedRevisions()
        {
            var revisionsAgeLimit = TimeSpan.FromSeconds(10);

            Action<RevisionsConfiguration> modifyConfiguration = configuration =>
                configuration.Collections["Users"] = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionAgeToKeep = revisionsAgeLimit
                };

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                //setup revisions on both stores and setup replication 
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database, modifyConfiguration);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database, modifyConfiguration);
                await SetupReplicationAsync(store1, store2);

                // create some revisions on store1
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");

                    await session.SaveChangesAsync();
                }

                for (int i = 2; i <= 10; i++)
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>("users/1-A");
                        user.Name = "Aviv" + i;
                        await session.SaveChangesAsync();
                    }
                }

                // wait for replication
                WaitForMarker(store1, store2);

                using (var session = store2.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<User>("users/1-A");
                    Assert.Equal("Aviv10", doc.Name);
                }

                // wait until revisions are expired
                await Task.Delay(revisionsAgeLimit);

                // modify document
                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1-A");
                    user.Name = "Grisha";
                    await session.SaveChangesAsync();
                }

                WaitForMarker(store1, store2);

                // expired revisions should be deleted
                // assert that both stores have just one revision now
                using (var session = store1.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>("users/1-A");
                    Assert.Equal(1, revisions.Count);
                    Assert.Equal("Grisha", revisions[0].Name);
                }
                using (var session = store2.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>("users/1-A");
                    Assert.Equal(1, revisions.Count);
                    Assert.Equal("Grisha", revisions[0].Name);
                }
            }

        }

        [Fact]
        public async Task ReplicateRevisionTombstones()
        {
            var revisionsAgeLimit = TimeSpan.FromSeconds(10);

            Action<RevisionsConfiguration> modifyConfiguration = configuration =>
                configuration.Collections["Users"] = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionAgeToKeep = revisionsAgeLimit
                };

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                //setup revisions on both stores and setup replication 
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database, modifyConfiguration);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database, modifyConfiguration);

                await SetupReplicationAsync(store1, store2);

                // create some revisions on store1
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");

                    await session.SaveChangesAsync();
                }

                for (int i = 2; i <= 10; i++)
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>("users/1-A");
                        user.Name = "Aviv" + i;
                        await session.SaveChangesAsync();
                    }
                }

                // wait for replication
                WaitForMarker(store1, store2);

                // wait until revisions are expired
                await Task.Delay(revisionsAgeLimit);

                // modify document
                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1-A");
                    user.Name = "Grisha";
                    await session.SaveChangesAsync();
                }

                WaitForMarker(store1, store2);

                // expired revisions should be deleted
                // assert that both stores have 10 revision tombstones

                foreach (var store in new[] { store1, store2 })
                {
                    var documentDatabase = await GetDocumentDatabaseInstanceFor(store);
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var tombstones = documentDatabase.DocumentsStorage.GetTombstonesFrom(ctx, 0, 0, int.MaxValue).ToList();
                        Assert.Equal(10, tombstones.Count);

                        foreach (var tombstone in tombstones)
                        {
                            Assert.Equal(Tombstone.TombstoneType.Revision, tombstone.Type);
                        }

                    }
                }

            }

        }


        [Fact]
        public async Task ClearRevisionsFlagAfterExpiration()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                foreach (var store in new[] { store1, store2 })
                {
                    await RevisionsHelper.SetupRevisions(store, Server.ServerStore, new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration
                        {
                            MinimumRevisionAgeToKeep = TimeSpan.FromSeconds(1),
                        }
                    });
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        var user = new User
                        {
                            Name = i.ToString()

                        };
                        await session.StoreAsync(user, "foo/bar");
                        await session.SaveChangesAsync();
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(5));

                await SetupReplicationAsync(store1, store2);

                WaitForMarker(store1, store2);

                WaitForUserToContinueTheTest(store2);

                using (var session = store2.OpenSession())
                {
                    var revisions = session.Advanced.Revisions.GetFor<User>("foo/bar");
                    Assert.Equal(0, revisions.Count);

                    var u = session.Load<User>("foo/bar");
                    var metadata = session.Advanced.GetMetadataFor(u);
                    metadata.TryGetValue(Constants.Documents.Metadata.Flags, out var flags);

                    Assert.DoesNotContain(nameof(DocumentFlags.HasRevisions), flags);
                }
            }
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
