using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class QueryPlanTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void QueryPlanForMultiUnaryMatch(Options options)
    {
        using var store = GetDocumentStore(options);
        new Index().Execute(store);
        using var session = store.OpenSession();
        session.Store(new Dto("maciej", new DateTime(2024, 8, 22)));
        session.SaveChanges();
        Indexes.WaitForIndexing(store);

        var result = session.Advanced.DocumentQuery<Dto, Index>()
            .WhereEquals(d => d.Name, "maciej")
            .AndAlso()
            .WhereBetween(x => x.Date, new DateTime(2024, 8, 21), new DateTime(2024, 8, 23))
            .Timings(out var timings)
            .ToList();

        Assert.NotNull(result);
        Assert.Equal(1, result.Count);
        Assert.NotNull(timings);
        Assert.NotNull(timings.QueryPlan);
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = docs => from d in docs
                select new { d.Name, d.Date };
        }
    }

    private record Dto(string Name, DateTime Date, string Id = null);
}
