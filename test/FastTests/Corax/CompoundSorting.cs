using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class CompoundSorting : RavenTestBase
{
    //We want to test if every comparer works good as second etc since it has different method to evaluate than single one.
    //Also Ascending/Descending
    public CompoundSorting(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void CompoundSorting_Spatial(bool boxedComparer)
    {
        using var store = GetDatabaseWithDocuments(out var indexName);
        using var session = store.OpenSession();

        var spatialAsc = GetBaseQuery(session, indexName, boxedComparer)
            .OrderByDistance(i => i.Spatial, 0, 0)
            .ToList()
            .Select(i => i.Id);
        
        Assert.Equal(new[]{"1", "2", "3"}, spatialAsc);
        
        var spatialDesc = GetBaseQuery(session, indexName, boxedComparer)
            .OrderByDistanceDescending(i => i.Spatial, 0, 0)
            .ToList()
            .Select(i => i.Id);
        
        Assert.Equal(new[]{"2", "1", "3"}, spatialDesc);

    }
    
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void CompoundSorting_Alphanumerical(bool boxedComparer)
    {
        using var store = GetDatabaseWithDocuments(out var indexName);
        using var session = store.OpenSession();

        var alphaNumericalAsc = GetBaseQuery(session, indexName, boxedComparer)
            .OrderBy(i => i.Alphanumeric, OrderingType.AlphaNumeric)
            .ToList()
            .Select(i => i.Id);
        
        Assert.Equal(new[]{"1", "2", "3"}, alphaNumericalAsc);
        
        var alphanumericalDesc = GetBaseQuery(session, indexName, boxedComparer)
            .OrderByDescending(i => i.Alphanumeric, OrderingType.AlphaNumeric)
            .ToList()
            .Select(i => i.Id);
        
        Assert.Equal(new[]{"2", "1", "3"}, alphanumericalDesc);

    }
    
     
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void CompoundSorting_Int(bool boxedComparer)
    {
        using var store = GetDatabaseWithDocuments(out var indexName);
        using var session = store.OpenSession();

        var intAsc = GetBaseQuery(session, indexName, boxedComparer)
            .OrderBy(i => i.Integer, OrderingType.Long)
            .ToList()
            .Select(i => i.Id);
        
        Assert.Equal(new[]{"1", "2", "3"}, intAsc);
        
        var intDesc = GetBaseQuery(session, indexName, boxedComparer)
            .OrderByDescending(i => i.Integer, OrderingType.Long)
            .ToList()
            .Select(i => i.Id);
        
        Assert.Equal(new[]{"2", "1", "3"}, intDesc);

    }
    
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void CompoundSorting_String_Floating(bool boxedComparer)
    {
        using var store = GetDatabaseWithDocuments(out var indexName);
        using var session = store.OpenSession();

        var queryStrAscFloatAsc = GetBaseQuery(session, indexName, boxedComparer)
            .OrderBy(nameof(Dto.Floating), OrderingType.Double)
            .ToList()
            .Select(i => i.Id);

        Assert.Equal(new[]{"1", "2", "3"}, queryStrAscFloatAsc);

        var queryStrAscFloatDesc = GetBaseQuery(session, indexName, boxedComparer)
            .OrderByDescending(nameof(Dto.Floating), OrderingType.Double)
            .ToList()
            .Select(i => i.Id);
        Assert.Equal(new[]{"2", "1", "3"}, queryStrAscFloatDesc);
    }

    private IDocumentStore GetDatabaseWithDocuments(out string indexName)
    { 
        var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var index = new SortingIndex();
        index.Execute(store);
        indexName = index.IndexName;
        using (var session = store.OpenSession())
        {
            var firstDoc = new Dto()
            {
                Id = "1",
                Name = "aaaa",
                Alphanumeric = "aaa1",
                Floating = 10.5,
                Integer = 1111,
                Lat = 10,
                Lon = 10
            };

            var secondDoc = new Dto()
            {
                Id = "2",
                Name = "aaaa",
                Alphanumeric = "aaa2",
                Floating = 11.5,
                Integer = 1112,
                Lat = 12,
                Lon = 12
            };

            var thirdDoc = new Dto()
            {
                Id = "3",
                Name = "bbbb",
                Alphanumeric = "aaa2",
                Floating = 12.5,
                Integer = 2222,
                Lat = 0,
                Lon = 0
            };
            session.Store(firstDoc);
            session.Store(secondDoc);
            session.Store(thirdDoc);
            session.SaveChanges();
        }
        Indexes.WaitForIndexing(store);
        return store;
    }

    private IDocumentQuery<Dto> GetBaseQuery(IDocumentSession session, string indexName, bool boxedComparer)
    {
        var query = session.Advanced.DocumentQuery<Dto>(indexName)
            .OrderBy(nameof(Dto.Name));

        return boxedComparer 
            ? query.OrderBy(nameof(Dto.Name)).OrderBy(nameof(Dto.Name)) //Up to three comparers are handled via generics, next are handled as interfaces
            : query;

    }
    
    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public double Floating { get; set; }
        public long Integer { get; set; }
        public object Spatial { get; set; }
        public double Lat { get; set; }
        public double Lon { get; set; }
        public string Alphanumeric { get; set; }
    }

    private class SortingIndex : AbstractIndexCreationTask<Dto>
    {
        public SortingIndex()
        {
            Map = docs => from doc in docs
                select new
                {
                    doc.Name, doc.Floating, doc.Integer, Spatial = CreateSpatialField(doc.Lat, doc.Lon), doc.Alphanumeric
                };
        }
    }

}
