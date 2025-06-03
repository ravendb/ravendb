using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_24132(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void CanGetVectorsFromIndex()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        
        using (var bulkInsert = store.BulkInsert())
        {
            for (int i = 0; i < 1000; i++)
            {
                bulkInsert.Store(new Dto(i.ToString()));
            }
        }
        
        var index = new Index();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        string from = string.Empty;

        List<string> all = new();
        do
        {
            var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Vector", from, 30));
            all.AddRange(terms);
            
            from = terms.LastOrDefault();
        } while (string.IsNullOrEmpty(from) == false);
        
        Assert.Equal(1000, all.Count);
        Assert.Distinct(all);

        using (var session = store.OpenSession())
            session.Query<Dto>().VectorSearch(x => x.WithText(p => p.Number), v => v.ByText("0")).ToList();
        
        WaitForUserToContinueTheTest(store);
    }
    
    private record Dto(string Number, string Id = null);

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => from dto in dtos
                select new { Vector = CreateVector(dto.Number) };
        }
    }
}
