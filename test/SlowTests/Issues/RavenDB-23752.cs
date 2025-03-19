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
                var dto1 = new Dto() { Name = "test a car" };
                var dto2 = new Dto() { Name = "test car" };
                session.Store(dto1);
                session.Store(dto2);
                session.SaveChanges();

                var index = new DummyIndex();
                index.Execute(store);
                Indexes.WaitForIndexing(store);
                
                var queryResults = session
                    .Query<DummyIndex.IndexEntry, DummyIndex>()
                    .Search(x => x.SearchedText, "test a car", @operator: SearchOperator.And)
                    .ProjectInto<Dto>()
                    .ToList();
                
                Assert.Equal(2, queryResults.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void ProximitySearchWithOnlyStopwordsReturnsEmptySet(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "of and" };
                var dto2 = new Dto() { Name = "of thisisnotastopword and" };
                session.Store(dto1);
                session.Store(dto2);
                session.SaveChanges();

                var result = session.Advanced
                    .DocumentQuery<Dto>()
                    .Search(x => x.Name, "of and")
                    .Proximity(3)
                    .ToList();
                
                Assert.Empty(result);
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
