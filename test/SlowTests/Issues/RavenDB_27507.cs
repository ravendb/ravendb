using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27507 : RavenTestBase
{
    public RavenDB_27507(ITestOutputHelper output) : base(output)
    {
    }

    // Both points sit in the level 1 geohash cell of longitude [0, 45) x latitude [0, 45), and the polygon covers that
    // cell whole - so the term generator hands it over as a term match and SpatialMatch accepts every entry under it
    // without reading it. That is where the distance used to be measured, so both documents came back with the same
    // score, and the closer one did not sort first.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ASpatialMatchMustScoreEntriesOfAFullyCoveredCell(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Place { Name = "closer", Lat = 22.5, Lng = 40.0 }); // 16.1 degrees from the centre
            session.Store(new Place { Name = "farther", Lat = 40.0, Lng = 22.5 }); // 17.5 degrees from the centre
            session.SaveChanges();
        }

        new Places_Score().Execute(store);
        Indexes.WaitForIndexing(store);

        using var read = store.OpenSession();

        var scores = read.Advanced
            .RawQuery<Row>(@"from index 'Places/Score' as p
                             where boost(p.Name = 'nothing', 2)
                                or spatial.within(p.Coordinates, spatial.wkt('POLYGON((-1 -1, 46 -1, 46 46, -1 46, -1 -1))'))
                             order by score()
                             select { Name: p.Name, Score: getMetadata(p)[""@index-score""] }")
            .ToList();

        Assert.Equal(2, scores.Count);
        Assert.NotEqual(scores.Single(x => x.Name == "closer").Score, scores.Single(x => x.Name == "farther").Score, 6);
        Assert.Equal("closer", scores[0].Name);
    }

    private class Row
    {
        public string Name { get; set; }

        public double Score { get; set; }
    }

    private class Place
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public double Lat { get; set; }

        public double Lng { get; set; }
    }

    private class Places_Score : AbstractIndexCreationTask<Place>
    {
        public Places_Score()
        {
            Map = places => from p in places
                            select new { p.Name, Coordinates = CreateSpatialField(p.Lat, p.Lng) };

            Index(x => x.Name, FieldIndexing.Exact);
            Configuration[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = "true";
        }
    }
}
