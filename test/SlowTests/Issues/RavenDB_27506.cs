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

public class RavenDB_27506 : RavenTestBase
{
    public RavenDB_27506(ITestOutputHelper output) : base(output)
    {
    }

    private const string Circle = "spatial.circle(120, 47.448, -122.309, 'miles')";

    // SpatialMatch collects a distance for every entry it returns, but reported IsBoosting = false, and
    // BinaryMatch.Score only asks a side for its score when that side says it has one - so the distance was
    // discarded and every document came back with Bm25Relevance.InitialScoreValue.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ASpatialLeafMustContributeItsDistanceToTheScore(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Place { Name = "near", Lat = 47.500, Lng = -122.300 }); // ~4 miles from the centre
            session.Store(new Place { Name = "far", Lat = 48.500, Lng = -122.000 });  // ~74 miles, still inside
            session.SaveChanges();
        }

        new Places_Score().Execute(store);
        Indexes.WaitForIndexing(store);

        var scores = Query(store, $"where boost(p.Name = 'nothing', 2) or spatial.within(p.Coordinates, {Circle})");

        Assert.Equal(2, scores.Count);
        Assert.NotEqual(scores.Single(x => x.Name == "near").Score, scores.Single(x => x.Name == "far").Score, 6);
    }

    private static List<Row> Query(IDocumentStore store, string where)
    {
        using var session = store.OpenSession();

        return session.Advanced
            .RawQuery<Row>($@"from index 'Places/Score' as p
                              {where}
                              order by score()
                              select {{ Name: p.Name, Score: getMetadata(p)[""@index-score""] }}")
            .ToList();
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
