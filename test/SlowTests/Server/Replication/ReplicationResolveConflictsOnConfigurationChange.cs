using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ReplicationResolveConflictsOnConfigurationChange : ReplicationTestBase
    {

        public async Task<List<ModifyOngoingTaskResult>> GenerateConflicts(DocumentStore store1, DocumentStore store2, string id = "foo/bar")
        {
            using (var session = store1.OpenSession())
            {
                session.Store(new User
                {
                    Name = "Store1"
                }, id);
                session.SaveChanges();
            }
            using (var session = store2.OpenSession())
            {
                session.Store(new User
                {
                    Name = "Store2"
                }, id);
                session.SaveChanges();
            }
            var list = await SetupReplicationAsync(store1,store2);
            list.AddRange(await SetupReplicationAsync(store2,store1));

            Assert.Equal(2, WaitUntilHasConflict(store1, id).Length);
            Assert.Equal(2, WaitUntilHasConflict(store2, id).Length);
            return list;
        }

        [Fact]
        public async Task ResolveWhenScriptAdded()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await GenerateConflicts(store1, store2);
                var config = new ConflictSolver
                {
                    ResolveByCollection = new Dictionary<string, ScriptResolver>
                    {
                        {
                            "Users", new ScriptResolver
                            {
                                Script = "return {'Name':'Resolved'}"
                            }
                        }
                    }
                };
                await SetupReplicationAsync(store1, config, store2);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Resolved"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Resolved"));
            }
        }

        [Fact]
        public async Task ResolveWhenChangeToLatest()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await GenerateConflicts(store1, store2);

                await SetReplicationConflictResolutionAsync(store1, StraightforwardConflictResolution.ResolveToLatest);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Store2"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Store2"));
            }
        }

        [Fact]
        public async Task ResolveWhenSettingDatabaseResolver()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await GenerateConflicts(store1, store2);
                var storage1 = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database).Result;

                var config = new ConflictSolver
                {
                    DatabaseResolverId = storage1.DbBase64Id
                };
                await SetupReplicationAsync(store1, config, store2);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Store1"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Store1"));
            }
        }

        [Fact]
        public async Task ResolveManyConflicts()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var list = await GenerateConflicts(store1, store2, "users/1");
                await DeleteOngoingTask(store1, list[0].TaskId, OngoingTaskType.Replication);
                await DeleteOngoingTask(store2, list[1].TaskId, OngoingTaskType.Replication);
                await GenerateConflicts(store1, store2, "users/2");
                var storage1 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                await UpdateConflictResolver(store1, storage1.DbBase64Id);

                Assert.True(WaitForDocument<User>(store1, "users/1", u => u.Name == "Store1"));
                Assert.True(WaitForDocument<User>(store2, "users/1", u => u.Name == "Store1"));
                Assert.True(WaitForDocument<User>(store1, "users/2", u => u.Name == "Store1"));
                Assert.True(WaitForDocument<User>(store2, "users/2", u => u.Name == "Store1"));
            }
        }
    }
}
