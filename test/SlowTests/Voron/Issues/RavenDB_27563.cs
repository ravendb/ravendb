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
using FreeSpaceHandling = Voron.Impl.FreeSpace.FreeSpaceHandling;
using Table = Voron.Data.Tables.Table;
using VoronConstants = Voron.Global.Constants;

namespace SlowTests.Voron.Issues;

public class RavenDB_27563 : RavenTestBase
{
    public RavenDB_27563(ITestOutputHelper output) : base(output)
    {
    }

    private const string Collection = "Orders";

    // Archived documents are always written through the compressed docs schema, regardless of the
    // collection's compression configuration (DocumentDatabase.GetDocsSchemaForCollection(collection, flags)).
    // Voron caches opened tables per (table name, schema.Compressed), so a write transaction that touches
    // both archived and plain documents of one collection holds TWO Table instances over the same physical
    // table. Each instance tracks its own active raw data section. Once one instance moves to a fresh section,
    // the other instance can release that section (Table.ReleaseNearlyEmptySection) and return its pages to
    // free space, while the first instance keeps treating it as its active section.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
    public async Task ArchivedAndPlainDocumentsInOneTransaction_ActiveSectionOfOneTableInstanceMustNotBeFreedByTheOther()
    {
        using var store = GetDocumentStore();
        var database = await GetDatabase(store.Database);
        var tableName = new CollectionName(Collection).GetTableName(CollectionTableType.Documents);
        var freeSpaceHandling = (FreeSpaceHandling)database.DocumentsStorage.Environment.FreeSpaceHandling;

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            // 1. a plain document creates the collection table and opens the non-compressed Table instance
            Put(context, "orders/plain", DocumentFlags.None, bodySize: 1024);

            var plainTable = tx.InnerTransaction.OpenTable(database.DocumentsStorage.DocsSchema, tableName);
            var archivedTable = tx.InnerTransaction.OpenTable(database.DocumentsStorage.CompressedDocsSchema, tableName);
            Assert.NotSame(plainTable, archivedTable);

            var initialSection = plainTable.ActiveDataSmallSection.PageNumber;
            Assert.Equal(initialSection, archivedTable.ActiveDataSmallSection.PageNumber);

            // 2. archived documents go through the compressed instance until it has to move to a new section
            var lastArchived = PutArchivedUntilSectionChanges(context, archivedTable, initialSection);

            var archivedSection = archivedTable.ActiveDataSmallSection.PageNumber;
            Assert.NotEqual(initialSection, archivedSection);
            // both instances of the same table share the active section within a transaction
            Assert.Equal(archivedSection, plainTable.ActiveDataSmallSection.PageNumber);

            // 3. a client re-saves the archived document that landed in the new section, with a bigger body.
            //    DocumentPutAction opens the table with the *incoming* flags (none), so this goes through the
            //    non-compressed instance. It cannot update in place, deletes the old value, finds a nearly empty
            //    section that it does not consider active, and releases it.
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

            // the compressed instance still considers that section to be its active one
            Assert.Equal(archivedSection, archivedTable.ActiveDataSmallSection.PageNumber);

            Assert.False(freedPages.Contains(archivedSection),
                $"The active section of the compressed Table instance (header page {archivedSection}) was returned to free space " +
                $"by the non-compressed Table instance of the same table, in the same transaction. Freed pages: {string.Join(", ", freedPages)}");
        }
    }

    // Same setup, one step further. The non-compressed instance relocates the document into an overflow page,
    // so only one of the freed pages is reused right away and the rest stay free. The compressed instance then
    // allocates the next archived document using its stale view of the released section, and reads pages
    // that no longer belong to it. Here the section was allocated and released within the same transaction, so
    // the data file was never extended to hold those pages and the pager rejects the read with a
    // VoronUnrecoverableErrorException. When the released pages do exist in the file (a section carved out of
    // free space on a long-lived database) they still carry an old raw data header, the write goes through
    // silently, and the document ends up on pages that are free for anyone to reuse.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
    public async Task ArchivedAndPlainDocumentsInOneTransaction_NextArchivedDocumentMustNotBeWrittenIntoFreedPages()
    {
        using var store = GetDocumentStore();
        var database = await GetDatabase(store.Database);
        var tableName = new CollectionName(Collection).GetTableName(CollectionTableType.Documents);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            Put(context, "orders/plain", DocumentFlags.None, bodySize: 1024);

            var plainTable = tx.InnerTransaction.OpenTable(database.DocumentsStorage.DocsSchema, tableName);
            var archivedTable = tx.InnerTransaction.OpenTable(database.DocumentsStorage.CompressedDocsSchema, tableName);
            var initialSection = plainTable.ActiveDataSmallSection.PageNumber;

            var lastArchived = PutArchivedUntilSectionChanges(context, archivedTable, initialSection);
            Assert.NotEqual(initialSection, archivedTable.ActiveDataSmallSection.PageNumber);

            // big enough to become a large value (overflow page) instead of going back into a small section
            Put(context, lastArchived, DocumentFlags.None, bodySize: 6000);

            // the next archived document is allocated by the compressed instance in the section it still holds
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

    private static string PutArchivedUntilSectionChanges(DocumentsOperationContext context, Table archivedTable, long initialSection)
    {
        string lastArchived = null;
        for (var i = 0; i < 100_000 && archivedTable.ActiveDataSmallSection.PageNumber == initialSection; i++)
        {
            lastArchived = $"orders/archived-{i}";
            Put(context, lastArchived, DocumentFlags.Archived, bodySize: 1024);
        }

        return lastArchived;
    }

    private static readonly Random Random = new(42);

    private static void Put(DocumentsOperationContext context, string id, DocumentFlags flags, int bodySize)
    {
        using var doc = context.ReadObject(new DynamicJsonValue
        {
            ["Body"] = RandomBody(bodySize),
            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = Collection
            }
        }, id);

        context.DocumentDatabase.DocumentsStorage.Put(context, id, expectedChangeVector: null, doc, flags: flags);
    }

    private static string RandomBody(int size)
    {
        // incompressible payload, so the entry size does not depend on which schema wrote it
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var chars = new char[size];
        for (var i = 0; i < chars.Length; i++)
            chars[i] = alphabet[Random.Next(alphabet.Length)];
        return new string(chars);
    }
}
