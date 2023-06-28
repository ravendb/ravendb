using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class ReplicationTests : InterversionTestBase
    {
        public ReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory(Skip = "TODO: Add compatible version of v4.2 when released")]
        public async Task CannotReplicateTimeSeriesToV42()
        {
            var version = "4.2.101"; // todo:Add compatible version of v4.2 when released
            var getOldStore = GetDocumentStoreAsync(version);
            await Task.WhenAll(getOldStore);

            using var oldStore = await getOldStore;
            using var store = GetDocumentStore();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Egor" }, "user/322");
                session.TimeSeriesFor("user/322", "a").Append(DateTime.UtcNow, 1);
                await session.SaveChangesAsync();
            }

            var externalTask = new ExternalReplication(oldStore.Database.ToLowerInvariant(), "MyConnectionString")
            {
                Name = "MyExternalReplication",
                Url = oldStore.Urls.First()
            };

            await SetupReplication(store, externalTask);

            var replicationLoader = (await Databases.GetDocumentDatabaseInstanceFor(store)).ReplicationLoader;
            Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
            Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true));
            Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))));
            Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("TimeSeries"))));
        }

        [Theory]
        [InlineData("4.2.117")]
        [InlineData("5.2.3")]
        public async Task CanReplicateToOldServerWithLowerReplicationProtocolVersion(string version)
        {
            // https://issues.hibernatingrhinos.com/issue/RavenDB-17346

            using var oldStore = await GetDocumentStoreAsync(version);
            using var store = GetDocumentStore();
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Egor" }, "users/1");
                    await session.SaveChangesAsync();
                }
            }

            var externalTask = new ExternalReplication(oldStore.Database.ToLowerInvariant(), "MyConnectionString")
            {
                Name = "MyExternalReplication",
                Url = oldStore.Urls.First()
            };

            await SetupReplication(store, externalTask);

            Assert.True(WaitForDocument<User>(oldStore, "users/1", u => u.Name == "Egor"));
        }

        [Fact]
        public async Task ShouldNotReplicateIncrementalTimeSeriesToOldServer()
        {
            const string version = "5.2.3";
            const string incrementalTsName = Constants.Headers.IncrementalTimeSeriesPrefix + "HeartRate";
            const string docId = "users/1";
            var baseline = DateTime.UtcNow;

            using (var oldStore = await GetDocumentStoreAsync(version))
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, docId);
                    session.IncrementalTimeSeriesFor(docId, incrementalTsName)
                        .Increment(baseline, 1);

                    await session.SaveChangesAsync();
                }

                await SetupReplication(store, oldStore);

                var replicationLoader = (await Databases.GetDocumentDatabaseInstanceFor(store)).ReplicationLoader;
                Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
                Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("IncrementalTimeSeries"))));
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task ExternalReplicationShouldWork_NonShardedAndV54()
        {
            var processNode = await GetServerAsync("5.4.5");
            var dbName = GetDatabaseName();
            using (var store = GetDocumentStore())
            using (var oldStore = await GetStore(processNode.Url, processNode.Process, dbName))
            {
                await InsertData(store, "$users/1");
                await InsertData(oldStore, "");

                var suffix = "$users/1";

                var externalTask = new ExternalReplication(oldStore.Database.ToLowerInvariant(), "MyConnectionString")
                {
                    Name = "MyExternalReplication",
                    Url = oldStore.Urls.First()
                };

                await SetupReplication(store, externalTask);
                await SetupReplication(oldStore, store);

                await EnsureReplicatingAsync(oldStore, store);
                await EnsureReplicatingAsync(store, oldStore);

                for (int i = 1; i < 5; i++)
                {
                    using (var oldSession = oldStore.OpenAsyncSession())
                    {
                        var id = $"users/{i}{suffix}";
                        var u = await oldSession.LoadAsync<User>(id);
                        Assert.NotNull(u);
                    }
                }

                for (int i = 1; i < 5; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var id = $"users/{i}";
                        var u = await session.LoadAsync<User>(id);
                        Assert.NotNull(u);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Replication)]
        public async Task ReplicationWithReshardingShouldWorkFromShardedToOldServer()
        {
            var processNode = await GetServerAsync("5.4.5");
            using (var store = Sharding.GetDocumentStore())
            using (var oldStore = await GetStore(processNode.Url, processNode.Process))
            {
                var suffix = "$usa";
                var id1 = $"users/1{suffix}";
                var id2 = $"users/2{suffix}";
                var id3 = $"users/3{suffix}";
                var id4 = $"users/4{suffix}";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, id1);
                    await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, id2);
                    await session.StoreAsync(new User { Name = "Name3", LastName = "LastName3", Age = 4 }, id3);
                    await session.StoreAsync(new User { Name = "Name4", LastName = "LastName4", Age = 15 }, id4);
                    await session.SaveChangesAsync();
                }
                
                var externalTask = new ExternalReplication(oldStore.Database.ToLowerInvariant(), "MyConnectionString")
                {
                    Name = "MyExternalReplication",
                    Url = oldStore.Urls.First()
                };

                await SetupReplication(store, externalTask);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id1);
                    user.AddressId = "New";
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument<User>(oldStore, id1, u => u.AddressId == "New"));

                var oldLocation = await Sharding.GetShardNumberForAsync(store, id1);
                await Sharding.Resharding.MoveShardForId(store, id1);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(4, tombs.Count);
                }

                for (var i = 1; i < 5; i++)
                {
                    var currentId = $"users/{i}{suffix}";
                    Assert.True(WaitForDocument<User>(oldStore, currentId, null));
                }

                var newLocation = await Sharding.GetShardNumberForAsync(store, id1);

                db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, newLocation));
                var storage = db.DocumentsStorage;

                var docsCount = storage.GetNumberOfDocuments();
                Assert.Equal(4, docsCount);
                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    //tombstones
                    var tombstonesCount = storage.GetNumberOfTombstones(context);
                    Assert.Equal(0, tombstonesCount);
                }

                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store.Database);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        public async Task ExternalReplicationWithRevisionTombstones_ShardedToOldServer()
        {
            var processNode = await GetServerAsync("5.4.5");
            var dbName = GetDatabaseName();
            using (var store = Sharding.GetDocumentStore())
            using (var oldStore = await GetStore(processNode.Url, processNode.Process, dbName))
            {
                await InsertData(store, "$users/1");
                await InsertData(oldStore, "");

                var suffix = "$users/1";
                var id = $"users/1{suffix}";

                var externalTask = new ExternalReplication(oldStore.Database.ToLowerInvariant(), "MyConnectionString")
                {
                    Name = "MyExternalReplication",
                    Url = oldStore.Urls.First()
                };

                await SetupReplication(store, externalTask);
                await SetupReplication(oldStore, store);

                await EnsureReplicatingAsync(oldStore, store);
                await EnsureReplicatingAsync(store, oldStore);

                var location = await Sharding.GetShardNumberForAsync(store, id);

                using (var s1 = store.OpenSession())
                {
                    s1.Delete(id);
                    s1.SaveChanges();
                }

                await EnsureReplicatingAsync(store, oldStore);
                await EnsureReplicatingAsync(store, oldStore);
                await EnsureReplicatingAsync(store, oldStore);

                await EnsureReplicatingAsync(oldStore, store);
                await EnsureReplicatingAsync(oldStore, store);
                await EnsureReplicatingAsync(oldStore, store);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, location));
                var storage = db.DocumentsStorage;
                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = storage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(5, tombs.Count);

                    int revisionTombsCount = 0, documentTombsCount = 0;
                    foreach (var item in tombs)
                    {
                        if (item is RevisionTombstoneReplicationItem)
                            revisionTombsCount++;
                        else if (item is DocumentReplicationItem)
                            documentTombsCount++;
                    }

                    Assert.Equal(4, revisionTombsCount); 
                    Assert.Equal(1, documentTombsCount);
                }

                await Task.Delay(3000);
                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store.Database);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        public async Task ExternalReplicationWithRevisionTombstones_NonShardedToOldServer()
        {
            var processNode = await GetServerAsync("5.4.5");
            var dbName = GetDatabaseName();
            using (var store = GetDocumentStore())
            using (var oldStore = await GetStore(processNode.Url, processNode.Process, dbName))
            {
                await InsertData(store, "$users/1");
                await InsertData(oldStore, "");

                var suffix = "$users/1";
                var id = $"users/1{suffix}";

                var externalTask = new ExternalReplication(oldStore.Database.ToLowerInvariant(), "MyConnectionString")
                {
                    Name = "MyExternalReplication",
                    Url = oldStore.Urls.First()
                };

                await SetupReplication(store, externalTask);
                await SetupReplication(oldStore, store);

                await EnsureReplicatingAsync(oldStore, store);
                await EnsureReplicatingAsync(store, oldStore);

                using (var s1 = store.OpenSession())
                {
                    s1.Delete(id);
                    s1.SaveChanges();
                }

                await EnsureReplicatingAsync(store, oldStore);
                await EnsureReplicatingAsync(store, oldStore);
                await EnsureReplicatingAsync(store, oldStore);

                await EnsureReplicatingAsync(oldStore, store);
                await EnsureReplicatingAsync(oldStore, store);
                await EnsureReplicatingAsync(oldStore, store);

                var db = await GetDocumentDatabaseInstanceFor(store, store.Database);
                var storage = db.DocumentsStorage;
                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = storage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(5, tombs.Count);

                    int revisionTombsCount = 0, documentTombsCount = 0;
                    foreach (var item in tombs)
                    {
                        if (item is RevisionTombstoneReplicationItem)
                            revisionTombsCount++;
                        else if (item is DocumentReplicationItem)
                            documentTombsCount++;
                    }

                    Assert.Equal(4, revisionTombsCount);
                    Assert.Equal(1, documentTombsCount);
                }
            }
        }

        internal static async Task<ModifyOngoingTaskResult> SetupReplication(IDocumentStore src, IDocumentStore dst)
        {
            var csName = $"cs-to-{dst.Database}";
            var result = await src.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = csName,
                Database = dst.Database,
                TopologyDiscoveryUrls = new[]
                {
                    dst.Urls.First()
                }
            }));
            Assert.NotNull(result.RaftCommandIndex);

            var op = new UpdateExternalReplicationOperation(new ExternalReplication(dst.Database.ToLowerInvariant(), csName)
            {
                Name = $"ExternalReplicationTo{dst.Database}",
                Url = dst.Urls.First()
            });

            return await src.Maintenance.SendAsync(op);
        }


        private static async Task<ModifyOngoingTaskResult> SetupReplication(IDocumentStore store, ExternalReplicationBase watcher)
        {
            var result = await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = watcher.ConnectionStringName,
                Database = watcher.Database,
                TopologyDiscoveryUrls = new[]
                {
                    watcher.Url
                }
            }));
            Assert.NotNull(result.RaftCommandIndex);

            IMaintenanceOperation<ModifyOngoingTaskResult> op;
            switch (watcher)
            {
                case PullReplicationAsSink pull:
                    op = new UpdatePullReplicationAsSinkOperation(pull);
                    break;
                case ExternalReplication ex:
                    op = new UpdateExternalReplicationOperation(ex);
                    break;
                default:
                    throw new ArgumentException($"Unrecognized type: {watcher.GetType().FullName}");
            }

            return await store.Maintenance.SendAsync(op);
        }

        protected async Task<DocumentStore> GetStore(string serverUrl, Process serverProcess = null, [CallerMemberName] string database = null, InterversionTestOptions options = null)
        {
            options = options ?? InterversionTestOptions.Default;
            var name = database ?? GetDatabaseName();

            if (options.ModifyDatabaseName != null)
                name = options.ModifyDatabaseName(name) ?? name;

            var store = new DocumentStore
            {
                Urls = new[] { serverUrl },
                Database = name
            };

            options.ModifyDocumentStore?.Invoke(store);

            store.Initialize();

            if (options.CreateDatabase)
            {
                var doc = new DatabaseRecord(name)
                {
                    Settings =
                    {
                        [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                        [RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter)] = "1",
                        [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString()
                    }
                };

                options.ModifyDatabaseRecord?.Invoke(doc);

                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, options.ReplicationFactor));
            }

            if (serverProcess != null)
            {
                store.AfterDispose += (sender, e) =>
                {
                    KillSlavedServerProcess(serverProcess);
                };
            }
            return store;
        }

        private static async Task InsertData(IDocumentStore store, string suffix = "$usa")
        {
            var id1 = $"users/1{suffix}";
            var id2 = $"users/2{suffix}";
            var id3 = $"users/3{suffix}";
            var id4 = $"users/4{suffix}";

            using (var session = store.OpenAsyncSession())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = true
                    }
                }));

                //Docs
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, id1);
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, id2);
                await session.StoreAsync(new User { Name = "Name3", LastName = "LastName3", Age = 4 }, id3);
                await session.StoreAsync(new User { Name = "Name4", LastName = "LastName4", Age = 15 }, id4);

                await session.SaveChangesAsync();
            }

            // revision
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(id1);
                user.Age = 10;
                await session.SaveChangesAsync();
            }
        }
    }
}
