using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Backups.Sharding;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public class ShardedBackupTestBase
    {
        internal readonly RavenTestBase _parent;

        public ShardedBackupTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public async Task InsertData(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await SetupRevisionsAsync(store, configuration: new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                });

                //Docs
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, "users/1");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, "users/2");
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName3", Age = 4 }, "users/3");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName4", Age = 15 }, "users/4");

                //Time series
                session.TimeSeriesFor("users/1", "Heartrate")
                    .Append(DateTime.Now, 59d, "watches/fitbit");
                session.TimeSeriesFor("users/3", "Heartrate")
                    .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");
                //counters
                session.CountersFor("users/2").Increment("Downloads", 100);
                //Attachments
                var names = new[]
                {
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt",
                    "profile.png",
                };
                await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                    session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                    session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                    await session.SaveChangesAsync();
                }
            }

            //tombstone + revision
            using (var session = store.OpenAsyncSession())
            {
                session.Delete("users/4");
                var user = await session.LoadAsync<User>("users/1");
                user.Age = 10;
                await session.SaveChangesAsync();
            }

            //subscription
            await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

            //Identity
            store.Maintenance.Send(new SeedIdentityForOperation("users", 1990));

            //CompareExchange
            var user1 = new User
            {
                Name = "Toli"
            };
            var user2 = new User
            {
                Name = "Mitzi"
            };

            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("cat/toli", user1, 0));
            var operationResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("cat/mitzi", user2, 0));
            await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("cat/mitzi", operationResult.Index));

            //Cluster transaction
            using (var session = store.OpenAsyncSession(new SessionOptions
                   {
                       TransactionMode = TransactionMode.ClusterWide
                   }))
            {
                var user5 = new User { Name = "Ayende" };
                await session.StoreAsync(user5, "users/5");
                await session.StoreAsync(new { ReservedFor = user5.Id }, "usernames/" + user5.Name);

                await session.SaveChangesAsync();
            }

            //Index
            await new Index().ExecuteAsync(store);
        }
        public ShardedRestoreSettings GenerateShardRestoreSettings(IReadOnlyCollection<string> backupPaths, ShardingConfiguration sharding)
        {
            var settings = new ShardedRestoreSettings
            {
                Shards = new Dictionary<int, SingleShardRestoreSetting>(backupPaths.Count),
            };

            var backupDirByShard = new SortedDictionary<int, string>();
            foreach (var dir in backupPaths)
            {
                var shardNumber = GetShardNumberFromBackupPath(dir);
                backupDirByShard.Add(shardNumber, dir);
            }

            foreach (var (shardNumber, dir) in backupDirByShard)
            {
                settings.Shards.Add(shardNumber, new SingleShardRestoreSetting
                {
                    ShardNumber = shardNumber,
                    FolderName = dir,
                    NodeTag = sharding.Shards[shardNumber].Members[0]
                });
            }
            return settings;
        }

        private int GetShardNumberFromBackupPath(string path)
        {
            var shardIndexPosition = path.LastIndexOf('$') + 1;
            
            if (char.IsDigit(path[shardIndexPosition]) == false)
                throw new ArgumentException($"Missing shard number after $ sign in backup path {path}. Expected a number but got '{path[shardIndexPosition]}'");

            int shardNumberLength = 1;
            while (shardIndexPosition + shardNumberLength < path.Length &&
                   char.IsDigit(path[shardIndexPosition + shardNumberLength]))
                shardNumberLength++;

            return int.Parse(path.Substring(shardIndexPosition, shardNumberLength));
        }

        private class AtomicGuard
        {
#pragma warning disable CS0649
            public string Id;
#pragma warning restore CS0649
        }

        public async Task CheckData(IDocumentStore store, RavenDatabaseMode dbMode = RavenDatabaseMode.Single, long expectedRevisionsCount = 28, string database = null)
        {
            long docsCount = default, tombstonesCount = default, revisionsCount = default;
            database ??= store.Database;
            if (dbMode == RavenDatabaseMode.Sharded)
            {
                await foreach (var shard in _parent.Sharding.GetShardsDocumentDatabaseInstancesFor(database))
                {
                    var storage = shard.DocumentsStorage;

                    docsCount += storage.GetNumberOfDocuments();
                    using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        tombstonesCount += storage.GetNumberOfTombstones(context);
                        revisionsCount += storage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    }

                    //Index
                    Assert.Equal(1, shard.IndexStore.Count);
                }
            }
            else
            {
                var db = await _parent.GetDocumentDatabaseInstanceFor(store, database);
                var storage = db.DocumentsStorage;

                docsCount = storage.GetNumberOfDocuments();
                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    tombstonesCount = storage.GetNumberOfTombstones(context);
                    revisionsCount = storage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                }

                //Index
                var indexes = await store.Maintenance.ForDatabase(database).SendAsync(new GetIndexesOperation(0, 128));
                Assert.Equal(1, indexes.Length);
            }

            //doc
            Assert.Equal(5, docsCount);

            //Assert.Equal(1, detailedStats.CountOfCompareExchangeTombstones); //TODO - test number of processed compare exchange tombstones  

            //tombstone
            Assert.Equal(1, tombstonesCount);

            //revisions
            Assert.Equal(expectedRevisionsCount, revisionsCount);

            //Subscriptions
            var subscriptionDocuments = await store.Subscriptions.GetSubscriptionsAsync(0, 10, database);
            Assert.Equal(1, subscriptionDocuments.Count);

            using (var session = store.OpenSession(database))
            {
                //Time series
                var val = session.TimeSeriesFor("users/1", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                val = session.TimeSeriesFor("users/3", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                //Counters
                var counterValue = session.CountersFor("users/2").Get("Downloads");
                Assert.Equal(100, counterValue.Value);
            }

            using (var session = store.OpenAsyncSession(database))
            {
                var attachmentNames = new[]
                {
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt",
                    "profile.png",
                };

                for (var i = 0; i < attachmentNames.Length; i++)
                {
                    var user = await session.LoadAsync<User>("users/" + (i + 1));
                    var metadata = session.Advanced.GetMetadataFor(user);

                    //Attachment
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);
                    var attachment = attachments[0];
                    Assert.Equal(attachmentNames[i], attachment.GetString(nameof(AttachmentName.Name)));
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    if (i == 0)
                    {
                        Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                    else if (i == 1)
                    {
                        Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                    else if (i == 2)
                    {
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                        Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                }

                await session.StoreAsync(new User() { Name = "Toli" }, "users|");
                await session.SaveChangesAsync();
            }
            //Identity
            using (var session = store.OpenAsyncSession(database))
            {
                var user = await session.LoadAsync<User>("users/1991");
                Assert.NotNull(user);
            }
            //CompareExchange
            using (var session = store.OpenAsyncSession(new SessionOptions { Database = database, TransactionMode = TransactionMode.ClusterWide }))
            {
                var user = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("cat/toli");
                Assert.NotNull(user);

                var u = await store.Operations.ForDatabase(database).SendAsync(new GetCompareExchangeValueOperation<AtomicGuard>("rvn-atomic/usernames/Ayende"));
                Assert.NotNull(u.Value);

                u = await store.Operations.ForDatabase(database).SendAsync(new GetCompareExchangeValueOperation<AtomicGuard>("rvn-atomic/users/5"));
                Assert.NotNull(u.Value);

                var user2 = await session.LoadAsync<User>("users/5");
                Assert.NotNull(user2);

                user2 = await session.LoadAsync<User>("usernames/Ayende");
                Assert.NotNull(user2);
            }
        }

        public Task<WaitHandle[]> WaitForBackupToComplete(IDocumentStore store, RavenServer server)
        {
            return WaitForBackupsToComplete(new[] { store }, [server]);
        }

        public Task<WaitHandle[]> WaitForBackupToComplete(IDocumentStore store)
        {
            return WaitForBackupsToComplete(new[] { store });
        }

        public async Task<WaitHandle[]> WaitForBackupsToComplete(IEnumerable<IDocumentStore> stores, List<RavenServer> servers = null)
        {
            var waitHandles = new List<WaitHandle>();
            foreach (var store in stores)
            {
                await foreach (var db in _parent.Sharding.GetShardsDocumentDatabaseInstancesFor(store, servers))
                {
                    BackupTestBase.FillBackupCompletionHandles(waitHandles, db);
                }
            }

            return waitHandles.ToArray();
        }

        public async Task<WaitHandle[]> WaitForBackupsToComplete(List<RavenServer> nodes, string database)
        {
            var waitHandles = new List<WaitHandle>();

            await foreach (var db in _parent.Sharding.GetShardsDocumentDatabaseInstancesFor(database, nodes))
            {
                BackupTestBase.FillBackupCompletionHandles(waitHandles, db);
            }

            return waitHandles.ToArray();
        }

        public Task<long> UpdateConfigurationAndRunBackupAsync(RavenServer server, IDocumentStore store, PeriodicBackupConfiguration config, bool isFullBackup = false)
        {
            return UpdateConfigurationAndRunBackupAsync(new List<RavenServer> { server }, store, config, isFullBackup);
        }

        public async Task<long> UpdateConfigurationAndRunBackupAsync(List<RavenServer> servers, IDocumentStore store, PeriodicBackupConfiguration config, bool isFullBackup = false)
        {
            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

            WaitForResponsibleNodeUpdateInCluster(store, servers, result.TaskId);

            await RunBackupAsync(store.Database, result.TaskId, isFullBackup, servers);

            return result.TaskId;
        }

        public async Task<long> UpdateConfigAsync(RavenServer server, PeriodicBackupConfiguration config, DocumentStore store)
        {
            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

            WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, result.TaskId);

            return result.TaskId;
        }

        public void WaitForResponsibleNodeUpdate(ServerStore serverStore, string databaseName, long taskId, string differentThan = null)
        {
            using (serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                using (var rawRecord = serverStore.Engine.StateMachine.ReadRawDatabaseRecord(context, databaseName, out _))
                {
                    foreach (var (name, _) in rawRecord.Topologies)
                    {
                        var value = WaitForValue(() =>
                        {
                            var responsibleNode = BackupUtils.GetResponsibleNodeTag(serverStore, name, taskId);
                            return responsibleNode != differentThan;
                        }, true);

                        Assert.True(value);
                    }
                }
            }
        }

        public void WaitForResponsibleNodeUpdateInCluster(IDocumentStore store, List<RavenServer> nodes, long backupTaskId)
        {
            foreach (var server in nodes)
            {
                WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, backupTaskId);
            }
        }

        public async Task RunBackupAsync(string database, long taskId, bool isFullBackup, List<RavenServer> servers = null)
        {
            var time = SystemTime.UtcNow;
            await foreach (var documentDatabase in _parent.Sharding.GetShardsDocumentDatabaseInstancesFor(database, servers))
            {
                var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
                periodicBackupRunner.StartBackupTask(taskId, isFullBackup, startTimeUtc: time);
            }
        }

        public IDisposable ReadOnly(string path)
        {
            var allFiles = new List<string>();
            var dirs = Directory.GetDirectories(path);
            FileAttributes attributes = default;
            foreach (string dir in dirs)
            {
                var files = Directory.GetFiles(dir);
                if (attributes != default)
                    attributes = new FileInfo(files[0]).Attributes;

                foreach (string file in files)
                {
                    File.SetAttributes(file, FileAttributes.ReadOnly);
                }

                allFiles.AddRange(files);
            }


            return new DisposableAction(() =>
            {
                foreach (string file in allFiles)
                {
                    File.SetAttributes(file, attributes);
                }
            });
        }

        private static async Task<long> SetupRevisionsAsync(
            IDocumentStore store,
            RevisionsConfiguration configuration)
        {
            if (store == null)
                throw new ArgumentNullException(nameof(store));

            var result = await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRevisionsOperation(configuration));
            return result.RaftCommandIndex ?? -1;
        }

        private class Item
        {

        }

        private class Index : AbstractIndexCreationTask<Item>
        {
            public Index()
            {
                Map = items =>
                    from item in items
                    select new
                    {
                        _ = new[]
                        {
                            CreateField("foo", "a"),
                            CreateField("foo", "b"),
                        }
                    };
            }
        }
    }
}
