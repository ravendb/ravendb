using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19641 : ReplicationTestBase
    {
        public RavenDB_19641(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task EnforceConfiguration_Should_Not_Delete_Conflicted_And_Resolved_Revisions()
        {
            using var source = GetDocumentStore();
            using var destination = GetDocumentStore();
        
        
            using (var session = source.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.StoreAsync(new User { Name = "Old" }, "Docs/2");
                await session.SaveChangesAsync();
            }

            using (var session = destination.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.StoreAsync(new User { Name = "New" }, "Docs/2");
                await session.SaveChangesAsync();
            }
        
            await SetupReplicationAsync(source, destination); // Conflicts resolved
            await EnsureReplicatingAsync(source, destination);
        
            using (var session = destination.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount);
                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(3, doc2RevCount);
            }
            
            var db = await Databases.GetDocumentDatabaseInstanceFor(destination);
            using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                 await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);

            using (var session = destination.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(3, doc1RevCount);
                Assert.Equal(3, doc2RevCount);
            }
        }

        [Fact]
        public async Task Enforce_Conflicted_PurgeDelete_Configuration_Should_Delete_Conflicted_And_Resolved_Revisions()
        {
            using var source = GetDocumentStore();
            using var destination = GetDocumentStore();

            using (var session = source.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.StoreAsync(new User { Name = "Old" }, "Docs/2");
                await session.SaveChangesAsync();
            }

            using (var session = destination.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.StoreAsync(new User { Name = "New" }, "Docs/2");
                await session.SaveChangesAsync();
            }

            await SetupReplicationAsync(source, destination); // Conflicts resolved
            await EnsureReplicatingAsync(source, destination);

            using (var session = destination.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount);
                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(3, doc2RevCount);
            }

            await destination.Maintenance.Server.SendAsync(new ConfigureRevisionsForConflictsOperation(destination.Database, new RevisionsCollectionConfiguration
            {
                PurgeOnDelete = true
            }));
            var db = await Databases.GetDocumentDatabaseInstanceFor(destination);
            using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);

            using (var session = destination.OpenAsyncSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(3, doc1RevCount);
                Assert.Equal(3, doc2RevCount);
            }

            using (var session = destination.OpenAsyncSession())
            {
                session.Delete("Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = destination.OpenAsyncSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(0, doc1RevCount);
                Assert.Equal(3, doc2RevCount);
            }
        }

        [Fact]
        public async Task EnforceConfiguration_Should_Not_Delete_Revisions()
        {
            using var source = GetDocumentStore();
        
            await RevisionsHelper.SetupRevisions(Server.ServerStore, source.Database);
        
            using (var session = source.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.StoreAsync(new User { Name = "Old" }, "Docs/2");
                await session.SaveChangesAsync();
            }
        
            using (var session = source.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.StoreAsync(new User { Name = "New" }, "Docs/2");
                await session.SaveChangesAsync();
            }
        
            using (var session = source.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(2, doc1RevCount);
                Assert.Equal(2, doc2RevCount);
            }
        
            var db = await Databases.GetDocumentDatabaseInstanceFor(source);
            using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);
        
        
            using (var session = source.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(2, doc1RevCount);
                Assert.Equal(2, doc2RevCount);
            }
        }

        [Fact]
        public async Task Enforce_PurgeDelete_Configuration_Should_Delete_Revisions()
        {
            using var store = GetDocumentStore();

            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = false,
                        MinimumRevisionsToKeep = 123
                    }
                }
            };

            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.StoreAsync(new User { Name = "Old" }, "Docs/2");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.StoreAsync(new User { Name = "New" }, "Docs/2");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(2, doc1RevCount);
                Assert.Equal(2, doc2RevCount);
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Delete("Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(3, doc1RevCount);
                Assert.Equal(2, doc2RevCount);
            }

            var newConfiguartion = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = true,
                        MinimumRevisionsToKeep = 123
                    }
                }
            };

            await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(newConfiguartion));
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(0, doc1RevCount);
                Assert.Equal(2, doc2RevCount);
            }
        }

        [Fact]
        public async Task DocRevisionsShouldObeyNewConigWhenChangingTheDoc()
        {
            using var store = GetDocumentStore();

            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(2, doc1RevCount);
            }

            configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = true,
                    MinimumRevisionsToKeep = 100,
                    MaximumRevisionsToDeleteUponDocumentUpdate = 1
                }
            };
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration);

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(2, doc1RevCount);
                var d = await session.LoadAsync<User>("Docs/1");
                var metadata = session.Advanced.GetMetadataFor(d);
                Assert.Equal((DocumentFlags.HasRevisions).ToString(), metadata[Raven.Client.Constants.Documents.Metadata.Flags]);
            }
            
            using (var session = store.OpenAsyncSession())
            {
                var d = await session.LoadAsync<User>("Docs/1");
                d.Name = "New2";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(0, doc1RevCount);
                var d = await session.LoadAsync<User>("Docs/1");
                var metadata = session.Advanced.GetMetadataFor(d);
                Assert.False(metadata.ContainsKey(Raven.Client.Constants.Documents.Metadata.Flags));
            }
        }

        [Fact]
        public async Task DocRevisionsShouldBeDeletedAfterRemoveConfigsWhenChangingTheDoc()
        {
            using var store = GetDocumentStore();

            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(2, doc1RevCount);
            }

            configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 0,
                    MaximumRevisionsToDeleteUponDocumentUpdate = 1
                }
            };
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration);

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(2, doc1RevCount);
                var d = await session.LoadAsync<User>("Docs/1");
                var metadata = session.Advanced.GetMetadataFor(d);
                Assert.Equal((DocumentFlags.HasRevisions).ToString(), metadata[Raven.Client.Constants.Documents.Metadata.Flags]);
            }

            using (var session = store.OpenAsyncSession())
            {
                var d = await session.LoadAsync<User>("Docs/1");
                d.Name = "New2";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(1, doc1RevCount);
                var d = await session.LoadAsync<User>("Docs/1");
                var metadata = session.Advanced.GetMetadataFor(d);
                Assert.Equal((DocumentFlags.HasRevisions).ToString(), metadata[Raven.Client.Constants.Documents.Metadata.Flags]);
            }

            using (var session = store.OpenAsyncSession())
            {
                var d = await session.LoadAsync<User>("Docs/1");
                d.Name = "New3";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(0, doc1RevCount);
                var d = await session.LoadAsync<User>("Docs/1");
                var metadata = session.Advanced.GetMetadataFor(d);
                Assert.False(metadata.ContainsKey(Raven.Client.Constants.Documents.Metadata.Flags));
            }
        }


        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
