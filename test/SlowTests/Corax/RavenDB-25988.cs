using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class RavenDB_25988(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanSortAlphanumericallyOnNonExistingTerms(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store);
        
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto, Index>()
            .OrderBy(x => x.Name, OrderingType.AlphaNumeric)
            .ToList();
        
        Assert.Equal(4, results.Count);
        Assert.Equal(2, results[0].Subnames.Length); // doc with removed terms
        Assert.Equal("London", results[1].Subnames[0].Name);
        Assert.Equal("London", results[2].Subnames[0].Name);
        Assert.Equal("Paris", results[3].Subnames[0].Name);


        var emptySearch = session.Query<Dto, Index>().Search(x => x.Name, " ").ToList();
        Assert.Empty(emptySearch);
        
        var dashSearch = session.Query<Dto, Index>().Search(x => x.Name, "-").ToList();
        Assert.Empty(dashSearch);
        
        var emptySearch2 = session.Query<Dto, Index>().Search(x => x.Name, "").ToList();
        Assert.Empty(emptySearch2);

        var emptyEquals = session.Advanced.DocumentQuery<Dto, Index>().WhereEquals(x => x.Name, "").ToList();
        Assert.Empty(emptyEquals);
        
        var dashEquals = session.Advanced.DocumentQuery<Dto, Index>().WhereEquals(x => x.Name, "-").ToList();
        Assert.Empty(dashEquals);
        
        var emptyEquals2 = session.Advanced.DocumentQuery<Dto, Index>().WhereEquals(x => x.Name, " ").ToList();
        Assert.Empty(emptyEquals2);
        
        var nonExisting = session.Advanced.DocumentQuery<Dto, Index>().Not.WhereExists(x => x.Name).ToList();
        Assert.Single(nonExisting);
        
        WaitForUserToContinueTheTest(store);
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanMultiSortAlphanumericallyOnNonExistingTerms(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store);
        using var session = store.OpenSession();
        
        {
            var results = session.Advanced.DocumentQuery<Dto, Index>()
                .OrderBy(x => x.Name, OrderingType.AlphaNumeric)
                .OrderBy(x => x.Value, OrderingType.Long)
                .ToList();
        
            Assert.Equal(4, results.Count);
            Assert.Equal(2, results[0].Subnames.Length); // doc with removed terms
            Assert.Equal("London", results[1].Subnames[0].Name);
            Assert.Equal(1, results[1].Value);
            Assert.Equal("London", results[2].Subnames[0].Name);
            Assert.Equal(2, results[2].Value);
            Assert.Equal("Paris", results[3].Subnames[0].Name);
        }

        {
            var results = session.Advanced.DocumentQuery<Dto, Index>()
                .OrderBy(x => x.Name, OrderingType.AlphaNumeric)
                .OrderByDescending(x => x.Value, OrderingType.Long)
                .ToList();
        
            Assert.Equal(4, results.Count);
            Assert.Equal(2, results[0].Subnames.Length); // doc with removed terms
            Assert.Equal("London", results[1].Subnames[0].Name);
            Assert.Equal(2, results[1].Value);
            Assert.Equal("London", results[2].Subnames[0].Name);
            Assert.Equal(1, results[2].Value);
            Assert.Equal("Paris", results[3].Subnames[0].Name);
        }
    }

    private void InsertDocuments(IDocumentStore store)
    {
        using var session = store.OpenSession();
        
        session.Store(new Dto()
        {
            Value = 2,
            Subnames = [new(){Name = "London"}]
        });
        
        session.Store(new Dto()
        {
            Value = 1,
            Subnames = [new(){Name = " "}, new(){Name = "-"}]
        });
        
        session.Store(new Dto()
        {
            Value = 2,
            Subnames = [new(){Name = "Paris"}]
        });
        
        session.Store(new Dto()
        {
            Value = 1,
            Subnames = [new(){Name = "London"}]
        });
        
        session.SaveChanges();
        new Index().Execute(store);
        Indexes.WaitForIndexing(store);
    }

    private class Dto
    {
        public string Name { get; set; }
        public int Value { get; set; }
        public Dto[] Subnames { get; set; }
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => from dto in dtos
                from sub in dto.Subnames
                select new
                {
                    sub.Name,
                    dto.Value,
                };
            
            Index(x => x.Name, FieldIndexing.Search);
        }
    }
}
