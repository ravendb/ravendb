using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using FreeSpaceHandling = Voron.Impl.FreeSpace.FreeSpaceHandling;
using RawDataSection = Voron.Data.RawData.RawDataSection;
using Table = Voron.Data.Tables.Table;
using VoronConstants = Voron.Global.Constants;

namespace SlowTests.Voron.Issues;

public class RavenDB_27563 : RavenTestBase
{
    public RavenDB_27563(ITestOutputHelper output) : base(output)
    {
    }

    private const string Collection = "Orders";

    // Archived documents are stored compressed even when their collection is not (RavenDB-19501). This used to be done by writing them
    // through CompressedDocsSchema. Voron caches opened tables per (table name, schema.Compressed), so a write transaction that touched
    // both archived and plain documents of one collection drove the same physical table through TWO Table instances, each tracking
    // its own active raw data section. Once one instance moved to a fresh section, the other could release it
    // (Table.ReleaseNearlyEmptySection) and return its pages to free space while the first one kept allocating from it.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
    public async Task ArchivedAndPlainDocumentsInOneTransaction_ActiveSectionMustNotBeReleased()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var collectionName = new CollectionName(Collection);
        var freeSpaceHandling = (FreeSpaceHandling)database.DocumentsStorage.Environment.FreeSpaceHandling;

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            // 1. a plain document creates the collection table
            Put(context, "orders/plain", DocumentFlags.None, bodySize: 1024);

            // the Table instance every put of this collection goes through, whatever the document's flags
            var table = tx.InnerTransaction.OpenTable(database.GetDocsSchemaForCollection(collectionName), collectionName.GetTableName(CollectionTableType.Documents));
            var initialSection = table.ActiveDataSmallSection.PageNumber;

            // 2. archived documents until the table has to move to a new section
            var lastArchived = PutArchivedUntilSectionChanges(context, initialSection);
            var archivedSection = SectionOf(context, lastArchived);
            Assert.NotEqual(initialSection, archivedSection);

            // 3. a client re-saves the archived document that landed in the new section with a bigger body. It cannot be updated
            //    in place, so the old value is deleted, and a nearly empty section is released unless it is the active one.
            var freedPages = new HashSet<long>();
            Action<long> onPageFreed = page => freedPages.Add(page);
            freeSpaceHandling.PageFreed += onPageFreed;
            try
            {
                Put(context, lastArchived, DocumentFlags.None, bodySize: 2048);
            }
            finally
            {
                freeSpaceHandling.PageFreed -= onPageFreed;
            }

            Assert.False(freedPages.Contains(archivedSection),
                $"The active section (header page {archivedSection}) of the collection table was returned to free space. Freed pages: {string.Join(", ", freedPages)}");

            // archived and plain documents went through the same instance, so it followed the move to the new section
            Assert.Equal(archivedSection, table.ActiveDataSmallSection.PageNumber);
        }
    }

    // Same setup, one step further. The re-saved document is relocated into an overflow page, so only one of the freed pages is
    // reused right away and the rest stay free. A second Table instance would then allocate the next archived document using its
    // stale view of the released section, on pages that no longer belong to it. Here the section was allocated and released within
    // the same transaction, so the data file was never extended to hold those pages and the pager rejects the read with a
    // VoronUnrecoverableErrorException. When the released pages do exist in the file (a section carved out of free space on a
    // long-lived database) they still carry an old raw data header, the write goes through silently, and the document ends up on
    // pages that are free for anyone to reuse.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
    public async Task ArchivedAndPlainDocumentsInOneTransaction_NextArchivedDocumentMustNotBeWrittenIntoFreedPages()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var collectionName = new CollectionName(Collection);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            Put(context, "orders/plain", DocumentFlags.None, bodySize: 1024);

            var table = tx.InnerTransaction.OpenTable(database.GetDocsSchemaForCollection(collectionName), collectionName.GetTableName(CollectionTableType.Documents));
            var initialSection = table.ActiveDataSmallSection.PageNumber;

            var lastArchived = PutArchivedUntilSectionChanges(context, initialSection);
            Assert.NotEqual(initialSection, SectionOf(context, lastArchived));

            // big enough to become a large value (overflow page) instead of going back into a small section
            Put(context, lastArchived, DocumentFlags.None, bodySize: 6000);

            const string afterRelease = "orders/archived-after-release";
            Put(context, afterRelease, DocumentFlags.Archived, bodySize: 1024);

            using (var doc = database.DocumentsStorage.Get(context, afterRelease))
            {
                Assert.NotNull(doc);
                var page = doc.StorageId / VoronConstants.Storage.PageSize;
                var freePages = database.DocumentsStorage.Environment.FreeSpaceHandling.AllPages(tx.InnerTransaction.LowLevelTransaction);
                Assert.DoesNotContain(page, freePages);
            }
        }
    }

    // the collection table is opened with the collection's (non-compressed) schema for every document, archived rows are compressed per row
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
    public async Task ArchivedDocumentInNonCompressedCollection_IsStoredCompressedUntilUnarchived()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var collectionName = new CollectionName(Collection);
        Assert.False(database.GetDocsSchemaForCollection(collectionName).Compressed);

        const string id = "orders/1";
        var body = new string('a', 2048);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            Put(context, id, DocumentFlags.None, body);
            var table = tx.InnerTransaction.OpenTable(database.GetDocsSchemaForCollection(collectionName), collectionName.GetTableName(CollectionTableType.Documents));
            Assert.False(IsCompressed(context, table, id));

            Put(context, id, DocumentFlags.Archived, body);
            Assert.True(IsCompressed(context, table, id));

            Put(context, id, DocumentFlags.None, body, NonPersistentDocumentFlags.Unarchive);
            Assert.False(IsCompressed(context, table, id));
            using (var doc = database.DocumentsStorage.Get(context, id))
                Assert.False(doc.Flags.Contain(DocumentFlags.Archived));

            tx.Commit();
        }

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<dynamic>(id);
            Assert.Equal(body, (string)doc.Body);
            Assert.False(session.Advanced.GetMetadataFor(doc).ContainsKey(Constants.Documents.Metadata.Archived));
        }
    }

    private static bool IsCompressed(DocumentsOperationContext context, Table table, string id)
    {
        using (var doc = context.DocumentDatabase.DocumentsStorage.Get(context, id))
            return table.GetInfoFor(doc.StorageId).IsCompressed;
    }

    private static string PutArchivedUntilSectionChanges(DocumentsOperationContext context, long initialSection)
    {
        for (var i = 0; i < 100_000; i++)
        {
            var id = $"orders/archived-{i}";
            Put(context, id, DocumentFlags.Archived, bodySize: 1024);
            if (SectionOf(context, id) != initialSection)
                return id;
        }

        throw new InvalidOperationException("the section never filled up");
    }

    private static long SectionOf(DocumentsOperationContext context, string id)
    {
        using (var doc = context.DocumentDatabase.DocumentsStorage.Get(context, id))
            return RawDataSection.GetSectionPageNumber(context.Transaction.InnerTransaction.LowLevelTransaction, doc.StorageId);
    }

    private static readonly Random Random = new(42);

    private static void Put(DocumentsOperationContext context, string id, DocumentFlags flags, int bodySize)
    {
        Put(context, id, flags, RandomBody(bodySize));
    }

    private static void Put(DocumentsOperationContext context, string id, DocumentFlags flags, string body, NonPersistentDocumentFlags nonPersistentFlags = NonPersistentDocumentFlags.None)
    {
        using var doc = context.ReadObject(new DynamicJsonValue
        {
            ["Body"] = body,
            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = Collection
            }
        }, id);

        context.DocumentDatabase.DocumentsStorage.Put(context, id, expectedChangeVector: null, doc, flags: flags, nonPersistentFlags: nonPersistentFlags);
    }

    private static string RandomBody(int size)
    {
        // incompressible payload, so the entry size does not depend on whether the row was compressed
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var chars = new char[size];
        for (var i = 0; i < chars.Length; i++)
            chars[i] = alphabet[Random.Next(alphabet.Length)];
        return new string(chars);
    }
}
