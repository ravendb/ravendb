using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_26787 : ReplicationTestBase
    {
        public RavenDB_26787(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        public async Task ReplicatedRevisionsShouldNotInflateDestinationDatabaseChangeVector()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            using (var storeC = GetDocumentStore())
            {
                var revisionsConfiguration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                };

                await RevisionsHelper.SetupRevisionsAsync(storeA, configuration: revisionsConfiguration);
                await RevisionsHelper.SetupRevisionsAsync(storeB, configuration: revisionsConfiguration);
                await RevisionsHelper.SetupRevisionsAsync(storeC, configuration: revisionsConfiguration);

                // history is authored on A
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "v1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    user.Name = "v2";
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                Assert.NotNull(await WaitForDocumentToReplicateAsync<User>(storeB, "users/1", 15_000));

                // B continues the history, so B's revisions carry A's change-vector entries
                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    user.Name = "v3";
                    await session.SaveChangesAsync();
                }

                // delete the document on B and purge its tombstone BEFORE B->C replication exists,
                // so the only items C can ever receive from B are revisions
                using (var session = storeB.OpenAsyncSession())
                {
                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }

                var databaseB = await Databases.GetDocumentDatabaseInstanceFor(storeB);
                await databaseB.TombstoneCleaner.ExecuteCleanup();

                using (databaseB.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(0, databaseB.DocumentsStorage.GetNumberOfTombstones(ctx));
                }

                var databaseA = await Databases.GetDocumentDatabaseInstanceFor(storeA);
                var dbIdA = databaseA.DbBase64Id;

                // sanity: the revisions survived the tombstone cleanup on B
                var statsB = await storeB.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(4, statsB.CountOfRevisionDocuments);

                // C has its own history, so its database change vector stays in Conflict with B's.
                // That keeps the batch-level wholesale merge of the source change vector
                // (MergedUpdateDatabaseChangeVectorCommand) out of the picture - only per-item
                // contributions can add foreign entries to C's frontier.
                using (var session = storeC.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "local" }, "locals/1");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeB, storeC);

                // C receives the revisions (v1, v2, v3 and the delete-revision) on top of its own local revision
                var revisionsOnC = await WaitForValueAsync(async () =>
                {
                    var stats = await storeC.Maintenance.SendAsync(new GetStatisticsOperation());
                    return stats.CountOfRevisionDocuments;
                }, 5, timeout: 60_000);
                Assert.Equal(5, revisionsOnC);

                var statsC = await storeC.Maintenance.SendAsync(new GetStatisticsOperation());

                // C never replicated with A and never received any live item from A,
                // so its database change vector must not claim A's lineage
                Assert.DoesNotContain(dbIdA, statsC.DatabaseChangeVector ?? string.Empty);
            }
        }
    }
}
