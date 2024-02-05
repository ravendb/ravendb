using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class SpatialBoostingInCorax : RavenTestBase
{
    public SpatialBoostingInCorax(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CanCoraxCalculateScoringForSpatial()
    {
        var docs = new List<SpatialDto>();
        docs.Add(new SpatialDto() {Lat = 0.01, Lon = 0.01});
        docs.Add(new SpatialDto() {Lat = 4, Lon = 4});
        docs.Add(new SpatialDto() {Lat = 2, Lon = 2});
        docs.Add(new SpatialDto() {Lat = 8, Lon = 8});
        docs.Add(new SpatialDto() {Lat = 15, Lon = 15});
        docs.Add(new SpatialDto() {Lat = 16, Lon = 16});

        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            foreach (var s in docs)
                session.Store(s);
            
            session.SaveChanges();
            new Index().Execute(store);
            Indexes.WaitForIndexing(store);
            var results = session.Query<SpatialDto, Index>().Customize(i => i.WaitForNonStaleResults())
                .Spatial("Spatial", factory => factory.WithinRadius(12 * 1.1512 * 60, 0, 0, SpatialUnits.Miles)).OrderByScore().ToList();
            
            Assert.Equal(4, results.Count);
            Assert.Equal(docs[0].Id, results[0].Id);
            Assert.Equal(docs[2].Id, results[1].Id);
            Assert.Equal(docs[1].Id, results[2].Id);
            Assert.Equal(docs[3].Id, results[3].Id);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CanCoraxCalculateScoringForSpatialDisjoint()
    {
        var docs = new List<SpatialDto>();
        docs.Add(new SpatialDto() {Lat = 0.01, Lon = 0.01});
        docs.Add(new SpatialDto() {Lat = 4, Lon = 4});
        docs.Add(new SpatialDto() {Lat = 2, Lon = 2});
        docs.Add(new SpatialDto() {Lat = 8, Lon = 8});
        docs.Add(new SpatialDto() {Lat = 15, Lon = 15});
        docs.Add(new SpatialDto() {Lat = 16, Lon = 16});

        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            foreach (var s in docs)
                session.Store(s);
            
            session.SaveChanges();
            new Index().Execute(store);
            Indexes.WaitForIndexing(store);
            var results = session.Query<SpatialDto, Index>().Customize(i => i.WaitForNonStaleResults())
                .Spatial("Spatial", factory => factory.Disjoint("POLYGON ((5 10, 10 5, 15 10, 10 15, 5 10))", SpatialUnits.Miles)).OrderByScore().ToList();
            Assert.Equal(5, results.Count);
            Assert.Equal(docs[0].Id, results[0].Id);
            Assert.Equal(docs[2].Id, results[1].Id);
            Assert.Equal(docs[1].Id, results[2].Id);
            Assert.Equal(docs[5].Id, results[3].Id);
            Assert.Equal(docs[4].Id, results[4].Id);
        }
    }
    private class Index : AbstractIndexCreationTask<SpatialDto>
    {
        public Index()
        {
            Map = dtos => dtos.Select(x => new {Spatial = CreateSpatialField(x.Lat, x.Lon)});
        }
    }

    private class SpatialDto()
    {
        public double Lat { get; set; }
        public double Lon { get; set; }
        public string Id { get; set; }
    }
}
