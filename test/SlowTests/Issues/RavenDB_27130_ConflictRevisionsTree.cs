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
using Voron.Util.Conversion;
using Xunit;
using SchemaRevisions = Raven.Server.Documents.Schemas.Revisions;

namespace SlowTests.Issues
{
    public class RavenDB_27130_ConflictRevisionsTree : ReplicationTestBase
    {
        public RavenDB_27130_ConflictRevisionsTree(ITestOutputHelper output) : base(output)
        {
        }

        private const string Id = "users/1";

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task ReplicatedRevisionTombstonesTakeTheirEtagsOutOfTheTree()
        {
            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();
            using var store3 = GetDocumentStore();

            var (conflictBefore, conflictAfter, revisionsAfter, state) =
                await ReplicateConflictRevisionsThenTheirTombstonesAsync(store1, store2, store3);

            Assert.True(conflictAfter < conflictBefore, "sanity: the tombstones should have removed conflict revisions on store2");
            Assert.Equal(revisionsAfter, state.RevisionsCount);
            Assert.Equal(conflictAfter, state.ConflictRevisions);
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public Task PruningAfterReplicatedTombstonesKeepsTheMinimum() => RunPruningRoundsAsync(minimumToKeep: 2);

        // A minimum above the number of conflict revisions prunes nothing, so the rounds themselves are
        // shown to produce what the case above prunes.
        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public Task AMinimumAboveTheCountPrunesNothing() => RunPruningRoundsAsync(minimumToKeep: 100);

        private async Task RunPruningRoundsAsync(int minimumToKeep)
        {
            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();
            using var store3 = GetDocumentStore();

            await ReplicateConflictRevisionsThenTheirTombstonesAsync(store1, store2, store3);

            await RevisionsHelper.SetupConflictedRevisionsAsync(store2,
                new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = minimumToKeep });

            await RunConflictRoundsAsync(store1, store2, rounds: 4);

            var (conflict, _) = await GetRevisionFlagCountsAsync(store2, Id);
            var state = ReadTreeState(await Databases.GetDocumentDatabaseInstanceFor(store2), Id);

            Assert.True(conflict >= Math.Min(minimumToKeep, 2), $"pruned below the minimum: {conflict} < {minimumToKeep}");
            Assert.Equal(conflict, state.ConflictRevisions);
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task PruningWithoutReplicatedTombstonesKeepsTheMinimum()
        {
            const int minimumToKeep = 2;

            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();

            await SetupKeepEverythingByDefaultAsync(store1);
            await SetupKeepEverythingByDefaultAsync(store2);
            await RevisionsHelper.SetupConflictedRevisionsAsync(store2,
                new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = minimumToKeep });

            await SetupReplicationAsync(store1, store2);
            await RunConflictRoundsAsync(store1, store2, rounds: 4);

            var (conflict, _) = await GetRevisionFlagCountsAsync(store2, Id);
            var state = ReadTreeState(await Databases.GetDocumentDatabaseInstanceFor(store2), Id);

            Assert.Equal(conflict, state.ConflictRevisions);
            Assert.True(conflict >= minimumToKeep, $"pruned below the minimum: {conflict} < {minimumToKeep}");
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task TheKeyDoesNotOutliveTheDocumentsRevisions()
        {
            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();

            await SetupRegularAndConflictRevisionsAsync(store1, store2, regularRevisions: 4);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store2);
            Assert.True(ReadTreeState(database, Id).ConflictTreeValues > 1, "sanity: the document has conflict revisions listed");

            using (var session = store2.OpenAsyncSession())
            {
                session.Delete(Id);
                await session.SaveChangesAsync();
            }

            await store2.Maintenance.SendAsync(new DeleteRevisionsOperation(Id));

            var (_, total) = await GetRevisionFlagCountsAsync(store2, Id);
            Assert.Equal(0, total);
            Assert.Equal(0, ReadTreeState(database, Id).ConflictTreeValues);
        }

        private async Task RunConflictRoundsAsync(DocumentStore remote, DocumentStore local, int rounds)
        {
            for (var i = 0; i < rounds; i++)
            {
                using (var session = local.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "local-" + i }, Id);
                    await session.SaveChangesAsync();
                }

                using (var session = remote.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "remote-" + i }, Id);
                    await session.SaveChangesAsync();
                }

                await EnsureReplicatingAsync(remote, local);
            }
        }

        private async Task<(long ConflictBefore, long ConflictAfter, long RevisionsAfter, TreeState State)>
            ReplicateConflictRevisionsThenTheirTombstonesAsync(DocumentStore store1, DocumentStore store2, DocumentStore store3)
        {
            await SetupKeepEverythingByDefaultAsync(store1);
            await SetupKeepEverythingByDefaultAsync(store2);
            await SetupKeepEverythingByDefaultAsync(store3);

            await SetupReplicationAsync(store3, store1);
            await RunConflictRoundsAsync(store3, store1, rounds: 3);

            var (conflict1, _) = await GetRevisionFlagCountsAsync(store1, Id);
            Assert.True(conflict1 > 0, "sanity: store1 has conflict revisions");

            await SetupReplicationAsync(store1, store2);
            await EnsureReplicatingAsync(store1, store2);

            var database2 = await Databases.GetDocumentDatabaseInstanceFor(store2);
            var (conflictBefore, _) = await GetRevisionFlagCountsAsync(store2, Id);
            Assert.True(conflictBefore > 0, "sanity: store2 received conflict revisions");
            Assert.Equal(conflictBefore, ReadTreeState(database2, Id).ConflictRevisions);

            await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(Id));
            await EnsureReplicatingAsync(store1, store2);

            var (conflictAfter, revisionsAfter) = await GetRevisionFlagCountsAsync(store2, Id);

            return (conflictBefore, conflictAfter, revisionsAfter, ReadTreeState(database2, Id));
        }

        private static Task SetupKeepEverythingByDefaultAsync(IDocumentStore store)
        {
            return RevisionsHelper.SetupRevisionsAsync(store, configuration: new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100_000 }
            });
        }

        private async Task SetupRegularAndConflictRevisionsAsync(DocumentStore store1, DocumentStore store2, int regularRevisions)
        {
            var configuration = new RevisionsConfiguration
            {
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100_000 }
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store1, configuration: configuration);
            await RevisionsHelper.SetupRevisionsAsync(store2, configuration: configuration);

            for (var i = 0; i < regularRevisions; i++)
            {
                using var session = store2.OpenAsyncSession();
                await session.StoreAsync(new User { Name = "v" + i }, Id);
                await session.SaveChangesAsync();
            }

            await SetupReplicationAsync(store1, store2);

            for (var i = 0; i < 2; i++)
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "from-remote-" + i }, Id);
                    await session.SaveChangesAsync();
                }

                await EnsureReplicatingAsync(store1, store2);

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "local-again-" + i }, Id);
                    await session.SaveChangesAsync();
                }
            }
        }

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

        // ConflictTreeValues counts the listing marker, ConflictRevisions does not.
        private readonly record struct TreeState(long ConflictTreeValues, long ConflictRevisions, long RevisionsCount);

        private static TreeState ReadTreeState(DocumentDatabase database, string id)
        {
            var revisionsStorage = database.DocumentsStorage.RevisionsStorage;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out Slice lowerId))
            using (revisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefix))
            {
                var conflictTree = context.Transaction.InnerTransaction.ReadTree(SchemaRevisions.ConflictRevisionsSlice);
                var values = conflictTree?.MultiCount(prefix) ?? 0;

                var revisionsTree = context.Transaction.InnerTransaction.ReadTree(SchemaRevisions.RevisionsCountSlice);
                long revisions = 0;
                if (revisionsTree != null && revisionsTree.TryRead(prefix, out var reader))
                    revisions = reader.ReadLittleEndianInt64();

                return new TreeState(values, values == 0 ? 0 : values - 1, revisions);
            }
        }
    }
}
