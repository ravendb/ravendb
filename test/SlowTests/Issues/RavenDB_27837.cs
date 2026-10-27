using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Voron.Data.RawData;
using Xunit;
using VoronConstants = Voron.Global.Constants;

namespace SlowTests.Issues;

public class RavenDB_27837 : RavenTestBase
{
    public RavenDB_27837(ITestOutputHelper output) : base(output)
    {
    }

    private class User
    {
    }

    private static readonly string BigSeries = new('a', 200);
    private static readonly string FillerSeries = new('f', 100);

    // Deleting a document writes a deleted range per series while it iterates the series' stats rows, and each deleted range
    // rewrites the stats row with a merged change vector. A row that grows is moved, and with a full active section the move
    // defrags the page, which shifts the next stats row of the same document under the iterator.
    [RavenFact(RavenTestCategory.TimeSeries)]
    public async Task DeletingDocumentMustNotReadStatsRowMovedByTheDeletion()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var baseline = DateTime.UtcNow;

        // the first rows of the section, written while etags have one digit, so the deletion makes the big row grow
        using (var session = store.OpenSession())
        {
            session.Store(new User(), "users/x");
            session.TimeSeriesFor("users/x", BigSeries).Append(baseline, 1);
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            session.TimeSeriesFor("users/x", "b").Append(baseline, 1);
            session.SaveChanges();
        }

        // a hole on the first page, so the defrag pass has room for the grown row there
        using (var session = store.OpenSession())
        {
            session.Store(new User(), "users/y");
            session.TimeSeriesFor("users/y", "y").Append(baseline, 1);
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            session.Delete("users/y");
            session.SaveChanges();
        }

        // fill the section to its last page, so the grown row cannot go to the tail of any page
        var fillers = 0;
        int room;
        while ((room = RowsThatStillFitAtTheTail(database)) > 0)
        {
            using var session = store.OpenSession();
            for (var i = 0; i < room; i++, fillers++)
            {
                var id = $"users/f{fillers:D5}";
                session.Store(new User(), id);
                session.TimeSeriesFor(id, FillerSeries).Append(baseline, 1);
            }

            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            session.Delete("users/x");
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            Assert.Null(session.Load<User>("users/x"));
            Assert.Null(session.TimeSeriesFor("users/x", "b").Get());
        }
    }

    // how many rows of the big series' size still fit at a page tail, from the last used page on
    private static unsafe int RowsThatStillFitAtTheTail(DocumentDatabase database)
    {
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenReadTransaction())
        {
            var llt = tx.InnerTransaction.LowLevelTransaction;
            var table = TimeSeriesStats.GetOrCreateTable(tx.InnerTransaction, new CollectionName("Users"));
            var sectionPage = table.ActiveDataSmallSection.PageNumber;
            var section = (RawDataSmallSectionPageHeader*)llt.GetPage(sectionPage).Pointer;

            var firstPage = (RawDataSmallPageHeader*)llt.GetPage(sectionPage + 1).Pointer;
            var bigRow = ((RawDataSection.RawDataEntrySizes*)((byte*)firstPage + sizeof(RawDataSmallPageHeader)))->AllocatedSize + sizeof(RawDataSection.RawDataEntrySizes);

            var rows = 0;
            for (var i = section->LastUsedPage; i < section->NumberOfPages; i++)
            {
                var page = (RawDataSmallPageHeader*)llt.GetPage(sectionPage + i + 1).Pointer;
                var tail = VoronConstants.Storage.PageSize - page->NextAllocation;
                if (tail > bigRow)
                    rows += Math.Max(1, tail / bigRow);
            }

            return rows;
        }
    }
}
