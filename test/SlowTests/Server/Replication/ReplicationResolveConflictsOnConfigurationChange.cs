using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ReplicationResolveConflictsOnConfigurationChange : ReplicationTestBase
    {
        public ReplicationResolveConflictsOnConfigurationChange(ITestOutputHelper output) : base(output)
        {
        }

        public async Task<List<ModifyOngoingTaskResult>> GenerateConflictsAndSetupMasterMasterReplication(DocumentStore store1, DocumentStore store2, string id = "foo/bar")
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
            var list = await SetupReplicationAsync(store1, store2);
            list.AddRange(await SetupReplicationAsync(store2, store1));

            Assert.Equal(2, WaitUntilHasConflict(store1, id).Length);
            Assert.Equal(2, WaitUntilHasConflict(store2, id).Length);
            return list;
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ResolveWhenScriptAdded(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var store1 = GetDocumentStore(options: options))
            using (var store2 = GetDocumentStore(options: options))
            {
                await GenerateConflictsAndSetupMasterMasterReplication(store1, store2);
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
                await UpdateConflictResolver(store1, config.ResolveByCollection, config.ResolveToLatest);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Resolved"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Resolved"));
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ResolveWhenChangeToLatest(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var store1 = GetDocumentStore(options: options))
            using (var store2 = GetDocumentStore(options: options))
            {
                await GenerateConflictsAndSetupMasterMasterReplication(store1, store2);

                await SetReplicationConflictResolutionAsync(store1, StraightforwardConflictResolution.ResolveToLatest);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Store2"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Store2"));

                var databaseName = options.DatabaseMode == RavenDatabaseMode.Single ? store1.Database : await Sharding.GetShardDatabaseNameForDocAsync(store1, "foo/bar");
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    context.OpenReadTransaction();
                    var count = database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    Assert.Equal(3, count);
                }
            }
        }
    }
}
