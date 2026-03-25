using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class RavenDB_25930(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void CompoundFieldShouldNotImpactQueryBuilderPlanWhenIsNotUsed()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession(new SessionOptions() { NoCaching = true }))
        {
            session.Store(new Dto { Name = "1", Count = 1 });
            session.Store(new Dto { Name = "1", Count = 2 });
            session.SaveChanges();

            var index = new Index();
            index.Execute(store);
            Indexes.WaitForIndexing(store);
        }

        using (var session = store.OpenSession(new SessionOptions() { NoCaching = true }))
        {
            QueryTimings normalTimings = null;
            _ = session.Query<Dto, Index>()
                .Customize(x => x.Timings(out normalTimings))
                .Where(x => x.Name == "1" && x.Count > 0)
                .ToList();
        
            Assert.NotNull(normalTimings);
            var queryPlan = (QueryInspectionNode)(normalTimings.QueryPlan);
            Assert.True(UnaryMatchExists(queryPlan));
        }

        using (var session = store.OpenSession(new SessionOptions() { NoCaching = true }))
        {
            QueryTimings orderByTimings = null;
            var result = session.Advanced.DocumentQuery<Dto, Index>()
                .Timings(out orderByTimings)
                .WhereEquals("Name", "1")
                .AndAlso()
                .WhereGreaterThan("Count", 0)
                .OrderBy("Count", OrderingType.Long)
                .ToList();

            Assert.NotEmpty(result);
            Assert.NotNull(orderByTimings);
            var queryPlan = (QueryInspectionNode)(orderByTimings.QueryPlan);
            Assert.True(UnaryMatchExists(queryPlan));
            Assert.Equal(1, result[0].Count);
            Assert.Equal(2, result[1].Count);
        }


        bool UnaryMatchExists(QueryInspectionNode current, int limit = 10)
        {
            Assert.True(limit >= 0); // recursive guardian 
            var currentOperation = current.Operation;
            if (currentOperation.Contains("UnaryMatch"))
                return true;

            foreach (var child in current.Children)
            {
                if (UnaryMatchExists(child, limit - 1))
                    return true;
            }

            return false;
        }
    }

    private class Dto
    {
        public string Name { get; set; }
        public int Count { get; set; }
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = docs => from doc in docs
                select new { doc.Name, doc.Count };

            CompoundField("Name", "Count");
        }
    }
}
