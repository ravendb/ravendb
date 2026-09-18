using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Voron.Data.RawData;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;
using VoronConstants = Voron.Global.Constants;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_27558 : RavenTestBase
    {
        public RavenDB_27558(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Payload { get; set; }
        }

        // Archived documents used to be rewritten through CompressedDocsSchema while regular writes to the same collection used DocsSchema.
        // When the transaction merger put an ArchiveDocumentsCommand and regular puts into one transaction, the same Voron table was
        // driven through two Table instances. The second one kept a stale section header and allocated past the end of a page.
        [RavenFact(RavenTestCategory.ExpirationRefresh | RavenTestCategory.Voron)]
        public async Task Archival_merged_with_regular_writes_must_not_corrupt_the_documents_table()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    // keep every queued command in the transaction the merger already started
                    record.Settings[RavenConfiguration.GetKey(x => x.TransactionMergerConfiguration.MaxTimeToWaitForPreviousTx)] = int.MaxValue.ToString();
                    record.Settings[RavenConfiguration.GetKey(x => x.TransactionMergerConfiguration.MaxTxSize)] = "1024";
                }
            });

            const int archivedDocs = 100; // enough to fill the 15 pages of the collection's first small-row section
            var random = new Random(27558);
            var archiveAt = DateTime.UtcNow.AddDays(-1).ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < archivedDocs; i++)
                {
                    // over 4 KB so it lives on an overflow page, compressible so the archived copy moves into the small-row section
                    var item = new Item { Payload = RandomText(random, "ACGT", 6000) };
                    await session.StoreAsync(item, $"items/{i:D4}");
                    session.Advanced.GetMetadataFor(item)[Constants.Documents.Metadata.ArchiveAt] = archiveAt;
                }

                await session.SaveChangesAsync();
            }

            await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 100_000 });
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            var tableName = new CollectionName("Items").GetTableName(CollectionTableType.Documents);

            long sectionPage;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                sectionPage = context.Transaction.InnerTransaction.OpenTable(database.DocumentsStorage.DocsSchema, tableName).ActiveDataSmallSection.PageNumber;
            }

            var blocker = new BlockingCommand();
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var blockerTask = database.TxMerger.Enqueue(blocker);
                Assert.True(blocker.Started.Wait(TimeSpan.FromSeconds(30)));

                // 1. regular write of a large document: opens the collection table and its section object without touching the section header
                var largeTask = database.TxMerger.Enqueue(new MergedPutCommand(Doc(context, "items/large-regular", RandomText(random, "abcdefghijklmnopqrstuvwxyz", 6000)), "items/large-regular", null, database));

                // 2. the archiver rewrites every due document compressed, filling the small-row pages
                var archiveTask = database.DataArchivist.ArchiveDocs();
                Assert.Equal(2, await WaitForValueAsync(() => database.TxMerger.NumberOfQueuedOperations, 2));

                // 3. regular write of a small document: allocates from the same section
                var smallTask = database.TxMerger.Enqueue(new MergedPutCommand(Doc(context, "items/small-regular", RandomText(random, "abcdefghijklmnopqrstuvwxyz", 3000)), "items/small-regular", null, database));

                // last in the queue, records the transaction it ran in
                var probe = new BlockingCommand();
                probe.Release.Set();
                var probeTask = database.TxMerger.Enqueue(probe);
                Assert.Equal(4, database.TxMerger.NumberOfQueuedOperations);

                blocker.Release.Set();
                await Task.WhenAll(blockerTask, largeTask, archiveTask, smallTask, probeTask);

                // everything above ran in one transaction
                Assert.Equal(blocker.TransactionId, probe.TransactionId);
            }

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                AssertNoPageAllocatedPastItsEnd(context.Transaction.InnerTransaction.LowLevelTransaction, sectionPage);
            }

            using (var session = store.OpenAsyncSession())
            {
                var archived = await session.LoadAsync<Item>(Enumerable.Range(0, archivedDocs).Select(i => $"items/{i:D4}"));
                Assert.All(archived.Values, item =>
                {
                    Assert.Equal(6000, item.Payload.Length);
                    Assert.True((bool)session.Advanced.GetMetadataFor(item)[Constants.Documents.Metadata.Archived]);
                });

                Assert.Equal(6000, (await session.LoadAsync<Item>("items/large-regular")).Payload.Length);
                Assert.Equal(3000, (await session.LoadAsync<Item>("items/small-regular")).Payload.Length);
            }
        }

        private static unsafe void AssertNoPageAllocatedPastItsEnd(LowLevelTransaction llt, long sectionPage)
        {
            var numberOfPages = ((RawDataSmallSectionPageHeader*)llt.GetPage(sectionPage).Pointer)->NumberOfPages;
            for (var i = 1; i <= numberOfPages; i++)
            {
                var page = (RawDataSmallPageHeader*)llt.GetPage(sectionPage + i).Pointer;
                Assert.True(page->NextAllocation <= VoronConstants.Storage.PageSize, $"page {page->PageNumber} allocated up to {page->NextAllocation}");
            }
        }

        private static BlittableJsonReaderObject Doc(JsonOperationContext context, string id, string payload)
        {
            return context.ReadObject(new DynamicJsonValue
            {
                [nameof(Item.Payload)] = payload,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Items"
                }
            }, id);
        }

        private static string RandomText(Random random, string alphabet, int length)
        {
            var sb = new StringBuilder(length);
            for (var i = 0; i < length; i++)
                sb.Append(alphabet[random.Next(alphabet.Length)]);
            return sb.ToString();
        }

        private class BlockingCommand : DocumentMergedTransactionCommand
        {
            public readonly ManualResetEventSlim Started = new();
            public readonly ManualResetEventSlim Release = new();
            public long TransactionId;

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                TransactionId = context.Transaction.InnerTransaction.LowLevelTransaction.Id;
                Started.Set();
                Release.Wait(TimeSpan.FromSeconds(60));
                return 1;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                throw new NotSupportedException();
            }
        }
    }
}
