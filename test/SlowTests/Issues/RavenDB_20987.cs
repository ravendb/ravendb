using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20987 : ReplicationTestBase
    {
        public RavenDB_20987(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RevisionTombstoneWithIdThatRequireEscaping(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                await RevisionsHelper.SetupRevisionsAsync(store1);

                var id = "users~shiran\r\n1";

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), id);
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "shiran";
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(new DeleteRevisionsOperation.Parameters
                {
                    DocumentIds = new[] { id }
                }));

                await EnsureReplicatingAsync(store1, store2);

                var documentDatabase = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, id);
                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombstones = documentDatabase.DocumentsStorage.GetTombstonesFrom(ctx, 0, 0, int.MaxValue).ToList();
                    Assert.Equal(2, tombstones.Count);
                    foreach (var tombstone in tombstones)
                        Assert.Equal(Tombstone.TombstoneType.Revision, tombstone.Type);
                }
            }
        }
    }
}
