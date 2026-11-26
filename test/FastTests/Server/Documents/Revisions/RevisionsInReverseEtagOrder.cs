using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Revisions;

public class RevisionsInReverseEtagOrder(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int NumberOfItems = 16;

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Compression, LicenseRequired = true)]
    [InlineData(RevisionsStorage.RevisionType.All, null)]
    [InlineData(RevisionsStorage.RevisionType.Regular, null)]
    [InlineData(RevisionsStorage.RevisionType.All, "Items")]
    public async Task CompressedRevisionStaysValidUntilTheNextOneIsRead(RevisionsStorage.RevisionType type, string collection)
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record => record.DocumentsCompression = new DocumentsCompressionConfiguration(compressRevisions: true)
        });
        await RevisionsHelper.SetupRevisionsAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < NumberOfItems; i++)
                await session.StoreAsync(new Item { Text = string.Concat(Enumerable.Repeat($"item {i} ", 200)) }, $"items/{i}");

            await session.SaveChangesAsync();
        }

        var database = await GetDatabase(store.Database);
        var revisionsStorage = database.DocumentsStorage.RevisionsStorage;

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var revisions = collection == null
                ? revisionsStorage.GetRevisionsInReverseEtagOrder(context, type, skip: 0, take: int.MaxValue)
                : revisionsStorage.GetRevisionsInReverseEtagOrderForCollection(context, type, collection, skip: 0, take: int.MaxValue);

            int count = 0;
            foreach (Document revision in revisions)
            {
                // the revision points into the decompressed buffer of its row, which must stay
                // allocated while the revision is the current one, even if the caller allocates
                byte[] id = revision.Id.AsSpan().ToArray();
                byte[] data = revision.Data == null ? null : DataBytes(revision);

                using (context.Transaction.InnerTransaction.Allocator.Allocate(1024, out ByteString other))
                    other.ToSpan().Fill(0xFF);

                Assert.Equal(id, revision.Id.AsSpan().ToArray());
                if (data != null)
                    Assert.Equal(data, DataBytes(revision));

                count++;
            }

            Assert.Equal(NumberOfItems, count);
        }
    }

    private static unsafe byte[] DataBytes(Document revision) => new ReadOnlySpan<byte>(revision.Data.BasePointer, revision.Data.Size).ToArray();

    private class Item
    {
        public string Text { get; set; }
    }
}
