using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23528(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineData(266)]
    [InlineData(787)]
    [InlineData(513)]
    public void SmallSetListWillBeAlignedTo256(int stored)
    {
        var vector = new[] { 0.1f, 0.1f };
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var bulkInsert = store.BulkInsert())
        {
            for (int i = 0; i < stored; ++i)
                bulkInsert.Store(new Dto(){Vector = vector });
        }

        using (var session = store.OpenSession())
        {
            var _ = session.Query<Dto>().VectorSearch(f => f.WithEmbedding(p => p.Vector),
                    v => v.ByEmbedding(vector))
                .ToList();
            Indexes.WaitForIndexing(store);
            store.Maintenance.Send(new StopIndexingOperation());
        }

        using (var bulkInsert = store.BulkInsert())
        {
            for (int i = 0;  i < 16; ++i)
                bulkInsert.Store(new Dto(){Vector = vector });
        }
        
        store.Maintenance.Send(new StartIndexingOperation());
        Indexes.WaitForIndexing(store);
    }

    private class Dto
    {
        public RavenVector<float> Vector { get; set; }
    }
}
