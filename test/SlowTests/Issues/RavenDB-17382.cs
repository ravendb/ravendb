using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Tests.Core.Utils.Entities;
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

        [Fact]
        public async Task ReplicateDocumentsFromDifferentCollectionsUpdate()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var ongoing = await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "John Dow", Age = 30}, "users/1");

                    session.SaveChanges();
                }

                WaitForDocumentToReplicate<User>(store2, "users/1", 15000);

                await DeleteOngoingTask(store1, ongoing[0].TaskId, OngoingTaskType.Replication);
                var tasks = await GetOngoingTasks(store1.Database);
                Assert.Equal(0, tasks.Count);

                using (var session = store1.OpenSession())
                {
                    session.Delete("users/1");

                    session.SaveChanges();
                }

                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                await database.TombstoneCleaner.ExecuteCleanup();

                using (var session = store1.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Toli"}, "users/1");

                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "John Dow", Age = 30 }, "Marker");

                    session.SaveChanges();
                }

                await WaitForDocumentToReplicateAsync<User>(store2, "Marker", 15000);

                var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);
                Assert.Equal(1, stats.CountOfDocumentsConflicts);

                var conflicts = (await store2.Commands().GetConflictsForAsync("users/1")).ToList();
                Assert.Equal(2, conflicts.Count);

            }
        }

        [Fact]
        public async Task ReplicateDocumentsFromDifferentCollectionsConflict()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
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

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "John Dow", Age = 30 }, "Marker");

                    session.SaveChanges();
                }

                await WaitForDocumentToReplicateAsync<User>(store2, "Marker", 15000);

                var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);
                Assert.Equal(1, stats.CountOfDocumentsConflicts);

                var conflicts = (await store2.Commands().GetConflictsForAsync("users/1")).ToList();
                Assert.Equal(2, conflicts.Count);

            }
        }

        private async Task<List<OngoingTask>> GetOngoingTasks(string name)
        {
            var tasks = new Dictionary<long, OngoingTask>();
            foreach (var server in Servers)
            {
                var handler = await InstantiateOutgoingTaskHandler(name, server);
                foreach (var task in handler.GetOngoingTasksInternal().OngoingTasksList)
                {
                    if (tasks.ContainsKey(task.TaskId) == false && task.TaskConnectionStatus != OngoingTaskConnectionStatus.NotOnThisNode)
                        tasks.Add(task.TaskId, task);
                }
            }
            return tasks.Values.ToList();
        }
    }
}
