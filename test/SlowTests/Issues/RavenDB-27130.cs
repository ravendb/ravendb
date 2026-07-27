using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;
using SchemaRevisions = Raven.Server.Documents.Schemas.Revisions;

namespace SlowTests.Issues
{
    // RavenDB-27130
    // Verifies the per-document counters that back the revisions counts, read directly from the Voron trees:
    //   * Schemas.Revisions.ConflictRevisionsCountSlice -> number of Conflicted/Resolved revisions of a doc
    //   * Schemas.Revisions.RevisionsCountSlice         -> number of (all) revisions of a doc
    // In both tests the expected numbers are derived from the actual stored revisions' @flags, so the trees
    // are checked against reality (not against hard-coded values).
    public class RavenDB_27130 : ReplicationTestBase
    {
        public RavenDB_27130(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task ConflictRevisionsCount_IsTracked_WhenAllRevisionsAreConflictRevisions()
        {
            const int rounds = 5;

            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();

            // Only the conflict-revisions config governs 'Users' (there is no regular revisions config), so every
            // revision created on the conflicting document is a Conflicted/Resolved revision.
            var conflictConfig = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionAgeToKeep = TimeSpan.FromDays(365) };
            await RevisionsHelper.SetupConflictedRevisionsAsync(store1, Server.ServerStore, conflictConfig);
            await RevisionsHelper.SetupConflictedRevisionsAsync(store2, Server.ServerStore, conflictConfig);

            const string id = "users/1";
            await SetupReplicationAsync(store1, store2);

            // One-way replication: store2 keeps a local (divergent) value while store1's value replicates in, so
            // every round is a fresh conflict that store2 resolves -> conflict/resolved revisions accumulate.
            for (var i = 0; i < rounds; i++)
            {
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "local-" + i }, id);
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "remote-" + i }, id);
                    await session.SaveChangesAsync();
                }

                await EnsureReplicatingAsync(store1, store2);
            }

            var (expectedConflict, expectedTotal) = await GetRevisionFlagCountsAsync(store2, id);

            // sanity: the scenario produced revisions, and ALL of them are conflict/resolved revisions
            Assert.True(expectedTotal > 0, "expected some revisions to be created");
            Assert.Equal(expectedTotal, expectedConflict);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store2);
            var (conflictCount, revisionsCount) = ReadCountsFromTrees(database, id);

            Assert.Equal(expectedConflict, conflictCount);
            Assert.Equal(expectedTotal, revisionsCount);
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task ConflictAndRegularRevisionsCounts_AreTracked()
        {
            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();

            const string id = "users/1";
            await SetupRegularAndConflictRevisionsAsync(store1, store2, id);

            var (expectedConflict, expectedTotal) = await GetRevisionFlagCountsAsync(store2, id);

            // sanity: this document has BOTH regular and conflict revisions
            Assert.True(expectedConflict > 0, "expected some conflict/resolved revisions");
            Assert.True(expectedTotal > expectedConflict, "expected some regular (non-conflict) revisions too");

            var database = await Databases.GetDocumentDatabaseInstanceFor(store2);
            var (conflictCount, revisionsCount) = ReadCountsFromTrees(database, id);

            Assert.Equal(expectedConflict, conflictCount);
            Assert.Equal(expectedTotal, revisionsCount);
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task Counts_AreTracked_AfterPruningByRevisionsConfiguration()
        {
            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();

            const string id = "users/1";
            await SetupRegularAndConflictRevisionsAsync(store1, store2, id);

            // tighten the collection config so most revisions (regular and conflict alike) get pruned, then
            // enforce the configuration to actually delete them.
            await store2.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 2 }
                }
            }));

            var operation = await store2.Operations.SendAsync(new EnforceRevisionsConfigurationOperation());
            await operation.WaitForCompletionAsync();

            var (expectedConflict, expectedTotal) = await GetRevisionFlagCountsAsync(store2, id);

            // sanity: pruning actually removed revisions (kept at most the configured amount)
            Assert.True(expectedTotal > 0 && expectedTotal <= 2, $"expected pruning to keep 1..2 revisions, but got {expectedTotal}");

            var database = await Databases.GetDocumentDatabaseInstanceFor(store2);
            var (conflictCount, revisionsCount) = ReadCountsFromTrees(database, id);

            Assert.Equal(expectedConflict, conflictCount);
            Assert.Equal(expectedTotal, revisionsCount);
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task Counts_AreTracked_AfterDirectDeleteRevisions()
        {
            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();

            const string id = "users/1";
            await SetupRegularAndConflictRevisionsAsync(store1, store2, id);

            // direct delete of all the document's revisions (regular and conflict alike)
            await store2.Maintenance.SendAsync(new DeleteRevisionsOperation(id));

            var (expectedConflict, expectedTotal) = await GetRevisionFlagCountsAsync(store2, id);

            // sanity: everything was deleted
            Assert.Equal(0, expectedTotal);
            Assert.Equal(0, expectedConflict);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store2);
            var (conflictCount, revisionsCount) = ReadCountsFromTrees(database, id);

            Assert.Equal(expectedConflict, conflictCount);
            Assert.Equal(expectedTotal, revisionsCount);
        }

        // Builds a document on store2 that has BOTH regular revisions (from normal updates) and Conflicted/
        // Resolved revisions (from resolving a replication conflict), under a keep-everything collection config.
        private async Task SetupRegularAndConflictRevisionsAsync(DocumentStore store1, DocumentStore store2, string id)
        {
            var configuration = new RevisionsConfiguration
            {
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 1000 }
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store1, configuration: configuration);
            await RevisionsHelper.SetupRevisionsAsync(store2, configuration: configuration);

            // regular revisions from normal updates on store2
            for (var i = 0; i < 4; i++)
            {
                using var session = store2.OpenAsyncSession();
                await session.StoreAsync(new User { Name = "v" + i }, id);
                await session.SaveChangesAsync();
            }

            // now create a conflict: store1 writes the same id divergently, then replicate -> store2 resolves it,
            // adding Conflicted/Resolved revisions on top of the regular ones.
            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "from-remote" }, id);
                await session.SaveChangesAsync();
            }

            await SetupReplicationAsync(store1, store2);
            await EnsureReplicatingAsync(store1, store2);
        }

        // number of (Conflicted|Resolved) revisions, and the total number of revisions, for a document -
        // taken from the actual stored revisions' @flags.
        private static async Task<(long Conflict, long Total)> GetRevisionFlagCountsAsync(IDocumentStore store, string id)
        {
            using var session = store.OpenAsyncSession();
            var metadata = await session.Advanced.Revisions.GetMetadataForAsync(id, pageSize: int.MaxValue);

            long conflict = metadata.Count(m =>
            {
                var flags = m.GetString(Constants.Documents.Metadata.Flags) ?? string.Empty;
                return flags.Contains(nameof(DocumentFlags.Conflicted)) || flags.Contains(nameof(DocumentFlags.Resolved));
            });

            return (conflict, metadata.Count);
        }

        // reads the per-document counters straight from the Voron trees, keyed by the document's lower-id prefix.
        private static (long Conflict, long Revisions) ReadCountsFromTrees(DocumentDatabase database, string id)
        {
            var revisionsStorage = database.DocumentsStorage.RevisionsStorage;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out Slice lowerId))
            using (revisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefix))
            {
                var conflictTree = context.Transaction.InnerTransaction.ReadTree(SchemaRevisions.ConflictRevisionsCountSlice);
                var conflict = conflictTree?.Read(prefix)?.Reader.ReadLittleEndianInt64() ?? 0;

                var revisionsTree = context.Transaction.InnerTransaction.ReadTree(SchemaRevisions.RevisionsCountSlice);
                var revisions = revisionsTree?.Read(prefix)?.Reader.ReadLittleEndianInt64() ?? 0;

                return (conflict, revisions);
            }
        }
    }
}
