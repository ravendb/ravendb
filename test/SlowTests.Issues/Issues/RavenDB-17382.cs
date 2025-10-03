using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17382 : ReplicationTestBase
    {
        public RavenDB_17382(ITestOutputHelper output) : base(output)
        {
        }

        public readonly string DbName = "TestDB" + Guid.NewGuid();

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicateDocumentsFromDifferentCollectionsUpdate(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                var ongoing = await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "John Dow", Age = 30 }, "users/1");

                    session.SaveChanges();
                }

                WaitForDocumentToReplicate<User>(store2, "users/1", 15000);

                await DeleteOngoingTask(store1, ongoing[0].TaskId, OngoingTaskType.Replication);
                var tasks = await Databases.GetOngoingTasks(store1.Database, Servers);
                Assert.Equal(0, tasks.Count);

                using (var session = store1.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "users/1");
                await database.TombstoneCleaner.ExecuteCleanup();

                using (var session = store1.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Toli" }, "users/1");

                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                var stats = await GetDatabaseStatisticsAsync(store2);
                var expectedDocuments = options.DatabaseMode == RavenDatabaseMode.Single ? 1 : 3;
                Assert.Equal(expectedDocuments, stats.CountOfDocuments);
                Assert.Equal(1, stats.CountOfDocumentsConflicts);

                var conflicts = (await store2.Commands().GetConflictsForAsync("users/1")).ToList();
                Assert.Equal(2, conflicts.Count);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicateDocumentsFromDifferentCollectionsConflict(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "John Dow", Age = 30 }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new Employee { FirstName = "Toli" }, "users/1");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                var stats = await GetDatabaseStatisticsAsync(store2);
                var expectedDocuments = options.DatabaseMode == RavenDatabaseMode.Single ? 1 : 3;
                Assert.Equal(expectedDocuments, stats.CountOfDocuments);
                Assert.Equal(1, stats.CountOfDocumentsConflicts);

                var conflicts = (await store2.Commands().GetConflictsForAsync("users/1")).ToList();
                Assert.Equal(2, conflicts.Count);
            }
        }
    }
}
