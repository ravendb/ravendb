using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23752 : RavenTestBase
{
    public RavenDB_23752(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void StopwordsAreSkippedInQueryBuilding(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto = new Dto() { Name = "test a car" };
                session.Store(dto);
                session.SaveChanges();

                var index = new DummyIndex();
                index.Execute(store);
                Indexes.WaitForIndexing(store);
                
                var queryResults = session
                    .Query<DummyIndex.IndexEntry, DummyIndex>()
                    .Search(x => x.SearchedText, "test a car", @operator: SearchOperator.And)
                    .ProjectInto<Dto>()
                    .ToList();
                
                Assert.Single(queryResults);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
    }
    
    private class DummyIndex : AbstractIndexCreationTask<Dto, DummyIndex.IndexEntry>
    {
        public class IndexEntry
        {
            public string SearchedText { get; set; }
        }

        public DummyIndex()
        {
            Map = dtos => from dto in dtos
                select new IndexEntry() { SearchedText = dto.Name };
            
            Index(x => x.SearchedText, FieldIndexing.Search);
        }
    }
}
