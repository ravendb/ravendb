using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Tests.Infrastructure.Utils;
using Voron;
using Voron.Data.Tables;
using Xunit;
using static Tests.Infrastructure.Utils.RevisionTestHelpers;

namespace InterversionTests.Revisions
{
    // Sharded coverage: legacy raw-CV rows injected per-shard, then orchestrator reads.
    public class ShardedRevisionsMixedTests : RavenTestBase
    {
        public ShardedRevisionsMixedTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        public async Task ShardedRevisions_AllFourEntities_RoundTripPostMigration()
        {
            using var store = Sharding.GetDocumentStore();

            await RevisionsHelper.SetupRevisionsAsync(store);

            const string docId = "users/sharded-1";

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v0" }, docId);
                await session.SaveChangesAsync();
            }
            for (int i = 1; i <= 2; i++)
            {
                using var session = store.OpenAsyncSession();
                var u = await session.LoadAsync<User>(docId);
                u.Name = "v" + i;
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>(docId);
                session.Advanced.Attachments.Store(u, "att-1",
                    new MemoryStream(new byte[] { 1, 2, 3, 4 }), "application/octet-stream");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>(docId);
                session.Advanced.Attachments.Delete(u, "att-1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var metadata = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
                Assert.True(metadata.Count >= 4,
                    $"Expected at least 4 revisions for sharded doc {docId}, got {metadata.Count}");
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Delete(docId);
                await session.SaveChangesAsync();
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        public async Task ShardedRevisions_LegacyRowsAcrossAllShards_ReadableViaOrchestrator()
        {
            using var store = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = StripHashedRevisionPkToken
            });
            await RevisionsHelper.SetupRevisionsAsync(store);

            var docIds = new[] { "users/shard-a", "users/shard-b", "users/shard-c", "users/shard-d", "users/shard-e" };
            foreach (var docId in docIds)
            {
                using var session = store.OpenAsyncSession();
                await session.StoreAsync(new User { Name = "seed" }, docId);
                await session.SaveChangesAsync();
            }

            var seededCvs = new System.Collections.Generic.Dictionary<string, string>();

            await foreach (var shardDb in Sharding.GetShardsDocumentDatabaseInstancesFor(store.Database))
            {
                using (shardDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (var tx = ctx.OpenWriteTransaction())
                {
                    foreach (var docId in docIds)
                    {
                        if (shardDb.DocumentsStorage.Get(ctx, docId) == null)
                            continue;

                        var compoundCv = RevisionTestHelpers.BuildCompound(
                            ctx,
                            order: ("A", DbA, 100),
                            version: ("B", DbB, 200));

                        SeedLegacyRevisionRow(ctx, shardDb, docId, "Users", compoundCv);
                        seededCvs[docId] = compoundCv.AsString();
                        break;
                    }
                    tx.Commit();
                }
            }

            Assert.True(seededCvs.Count >= 1,
                "Expected at least one shard to host a doc; sharded routing assigned all docs to one shard?");

            foreach (var (docId, cvString) in seededCvs)
            {
                using var session = store.OpenAsyncSession();
                var versionOnly = cvString.Split('|')[1];
                var revision = await session.Advanced.Revisions.GetAsync<User>(versionOnly);
                Assert.NotNull(revision);
                Assert.Equal("Legacy", revision.Name);
            }
        }

        private static unsafe void SeedLegacyRevisionRow(
            DocumentsOperationContext context,
            DocumentDatabase database,
            string id,
            string collection,
            Raven.Server.Utils.ChangeVector compoundCv)
        {
            var collectionName = database.DocumentsStorage.ExtractCollectionName(context, collection);
            var table = database.DocumentsStorage.RevisionsStorage.EnsureRevisionTableCreated(
                context.Transaction.InnerTransaction, collectionName);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKeyForBackwardCompatibility(context, id, out Slice lowerId, out Slice idSlice))
            using (RevisionsStorage.BuildRevisionKey(context.Allocator, compoundCv, out var key))
            using (var docBlittable = BuildBlittableDocument(context, name: "Legacy"))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(key.Raw.Content.Ptr, key.Raw.Size);          // 0 raw cv.Version (legacy shape)
                tvb.Add(lowerId);                                              // 1
                tvb.Add(SpecialChars.RecordSeparator);                         // 2
                tvb.Add(Bits.SwapBytes(database.DocumentsStorage.GenerateNextEtag())); // 3
                tvb.Add(idSlice);                                              // 4
                tvb.Add(docBlittable.BasePointer, docBlittable.Size);          // 5
                tvb.Add((int)DocumentFlags.Revision);                          // 6
                tvb.Add(0L);                                                   // 7 NotDeletedRevisionMarker
                var ticks = DateTime.UtcNow.Ticks;
                tvb.Add(ticks);                                                // 8
                tvb.Add(context.GetTransactionMarker());                       // 9
                tvb.Add(0);                                                    // 10 Resolved
                tvb.Add(Bits.SwapBytes(ticks));                                // 11

                table.Insert(tvb);
            }
        }

        private static BlittableJsonReaderObject BuildBlittableDocument(DocumentsOperationContext context, string name)
        {
            var djv = new DynamicJsonValue
            {
                ["Name"] = name,
                [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = "Users"
                }
            };
            return context.ReadObject(djv, "doc");
        }
    }
}
