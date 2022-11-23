using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19563 : RavenTestBase
    {
        public RavenDB_19563(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Snapshot_should_have_correct_index_entries_after_snapshot_restore()
        {
            var backupPath = NewDataPath();
            IOExtensions.DeleteDirectory(backupPath);

            using (var store = GetDocumentStore())
            {
                const string id = "users/1";

                await new UsersIndex().ExecuteAsync(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grisha"
                    }, id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                database.ForTestingPurposesOnly().AfterSnapshotOfDocuments = () =>
                {
                    using (var session = store.OpenSession())
                    {
                        session.Advanced.WaitForIndexesAfterSaveChanges(TimeSpan.FromSeconds(10));
                        session.Delete(id);
                        session.SaveChanges();
                    }
                };

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = databaseName
                }))
                {
                    Indexes.WaitForIndexing(store, databaseName);

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var user = await session.LoadAsync<User>("users/1");
                        Assert.NotNull(user);

                        var usersCount = await session.Query<User, UsersIndex>().CountAsync();
                        Assert.Equal(1, usersCount);
                    }
                }
            }
        }

        [Fact]
        public async Task Snapshot_should_have_correct_index_entries_after_snapshot_and_incremental_restore()
        {
            var backupPath = NewDataPath();
            IOExtensions.DeleteDirectory(backupPath);

            using (var store = GetDocumentStore())
            {
                const string id = "users/1";

                await new UsersMapReduceIndex().ExecuteAsync(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grisha"
                    }, id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                long cleanedTombstones = 0;
                database.ForTestingPurposesOnly().BeforeSnapshotOfDocuments = () =>
                {
                    using (var session = store.OpenSession())
                    {
                        session.Advanced.WaitForIndexesAfterSaveChanges();
                        session.Delete(id);
                        session.SaveChanges();
                    }

                    cleanedTombstones = database.TombstoneCleaner.ExecuteCleanup().GetAwaiter().GetResult();
                };

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                Assert.Equal(0, cleanedTombstones);

                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = databaseName
                }))
                {
                    Indexes.WaitForIndexing(store, databaseName);

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var user = await session.LoadAsync<User>("users/1");
                        Assert.Null(user);

                        var usersCount = await session.Query<User, UsersMapReduceIndex>().CountAsync();
                        Assert.Equal(0, usersCount);
                    }
                }
            }
        }

        private class UsersIndex : AbstractIndexCreationTask<User>
        {
            public UsersIndex()
            {
                Map = users => from user in users
                               select new User
                               {
                                   Name = user.Name
                               };
            }
        }

        private class UsersMapReduceIndex : AbstractIndexCreationTask<User>
        {
            public UsersMapReduceIndex()
            {
                Map = users => from user in users
                    select new User
                    {
                        Name = user.Name,
                        Count = 1
                    };

                Reduce = results => from result in results
                    group result by new { result.Name } into g
                    select new
                    {
                        Name = g.Key.Name,
                        Count = g.Sum(x => x.Count)
                    };
            }
        }
    }
}
