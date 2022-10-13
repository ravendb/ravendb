using System;
using System.Linq;
using Corax;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19426 : RavenTestBase
{
    public RavenDB_19426(ITestOutputHelper output) : base(output)
    {
    }


    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DeletionOfItemFromFanoutIndexReturnsCorrectCountOfTerms(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new FanoutIndex.Multi() {Name = "First", Data = new[] {new[] {"test", "untested"}, new[] {"untested", "test"}}});
            session.SaveChanges();
        }
        var index = new FanoutIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
        Assert.Equal(2, indexStats.EntriesCount);

        {
            //Swap in place, no new item.
            using var session = store.OpenSession();
            var result = session.Query<FanoutIndex.Multi, FanoutIndex>().Where(i => i.Name == "First").First();
            session.Delete(result);
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
        Assert.Equal(0, indexStats.EntriesCount);
    }
    
   

    private class FanoutIndex : AbstractIndexCreationTask<FanoutIndex.Multi>
    {
        public class Multi
        {
            public string Name { get; set; }
            public string[][] Data { get; set; }
        }
        
        public FanoutIndex()
        {
            Map = multis =>
                from doc in multis
                from d in doc.Data
                select new
                {
                    Name = doc.Name,
                    Data = d.Select(i => i)
                };
        }
    }
}
