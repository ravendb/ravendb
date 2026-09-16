using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27460 : RavenTestBase
{
    public RavenDB_27460(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ANegatedTermMustNotContributeToTheScore(Options options)
    {
        const int documents = 200_000; // 'Planned' appears ~133k times - above the stored relevance threshold

        using var store = GetDocumentStore(options);

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < documents; i++)
            {
                bulk.Store(new Movie
                {
                    Genre = i % 100 == 0 ? "Drama" : "Filler",
                    Status = i % 3 == 0 ? "Released" : "Planned",
                    Title = i % 50 == 0 ? "the matrix reloaded" : "some other movie title"
                });
            }
        }

        new Movies_Score().Execute(store);
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(15));

        using var session = store.OpenSession();

        // 'Planned' documents enter through the search() branch while holding the negated term,
        // 'Released' ones satisfy the negated clause - both match the same two scoring terms.
        var hits = session.Advanced
            .RawQuery<Hit>(@"from index 'Movies/Score' as m
                             where (m.Genre = 'Drama' and m.Status != 'Planned') or search(m.Title, 'matrix')
                             order by score()
                             select { Status: m.Status, Score: getMetadata(m)[""@index-score""] }
                             limit 8192")
            .ToList();

        double satisfying = hits.Where(x => x.Status == "Released").Max(x => x.Score);
        double holdingNegatedTerm = hits.Where(x => x.Status == "Planned").Max(x => x.Score);

        Assert.True(satisfying >= holdingNegatedTerm,
            $"a document holding the negated term scored {holdingNegatedTerm} against {satisfying} for one that satisfies the clause");
    }

    // A spatial leaf inside an OR stays in the pipeline instead of being lifted to a post-filter (see
    // SpatialMatch.Inspect), so a negated one reaches CompiledQueryMatch.Score, which calls every resolved leaf
    // without checking whether it carries score data. Both 'hit' documents sit inside the circle, so neither
    // satisfies the negated clause and both match the boosted term exactly once - their scores must be equal.
    // A negated leaf that still scores gives them its distance to the circle's centre instead.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ANegatedSpatialClauseMustNotContributeToTheScore(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Place { Name = "hit", Lat = 47.500, Lng = -122.300 }); // ~4 miles from the centre
            session.Store(new Place { Name = "hit", Lat = 48.500, Lng = -122.000 }); // ~74 miles, still inside
            session.Store(new Place { Name = "miss", Lat = 10.000, Lng = 10.000 });  // outside, enters through the negated branch
            session.SaveChanges();
        }

        new Places_Score().Execute(store);
        Indexes.WaitForIndexing(store);

        using var read = store.OpenSession();

        var hits = read.Advanced
            .RawQuery<Hit>(@"from index 'Places/Score' as p
                             where boost(p.Name = 'hit', 2)
                                or not spatial.within(p.Coordinates, spatial.circle(120, 47.448, -122.309, 'miles'))
                             order by score()
                             select { Status: p.Name, Score: getMetadata(p)[""@index-score""] }")
            .ToList()
            .Where(x => x.Status == "hit")
            .Select(x => x.Score)
            .ToList();

        Assert.Equal(2, hits.Count);
        Assert.Equal(hits[0], hits[1], 6);
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

    private class Movie
    {
        public string Genre { get; set; }

        public string Status { get; set; }

        public string Title { get; set; }
    }

    private class Hit
    {
        public string Status { get; set; }

        public double Score { get; set; }
    }

    private class Movies_Score : AbstractIndexCreationTask<Movie>
    {
        public Movies_Score()
        {
            Map = movies => from m in movies
                            select new { m.Genre, m.Status, m.Title };

            Index(x => x.Genre, FieldIndexing.Exact);
            Index(x => x.Status, FieldIndexing.Exact);
            Index(x => x.Title, FieldIndexing.Search);
            Configuration[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = "true";
        }
    }
}
