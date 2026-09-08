using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26957(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Movie
    {
        public string Id { get; set; }
        public string Title { get; set; }
        public long Runtime { get; set; }
        public double Rating { get; set; }
    }

    private class Movies_Showcase : AbstractIndexCreationTask<Movie>
    {
        public Movies_Showcase()
        {
            Map = movies => from m in movies
                            select new
                            {
                                m.Title,
                                m.Runtime,
                                m.Rating
                            };
        }
    }

    private IDocumentStore SetupStore(Options options)
    {
        var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Movie { Id = "movies/1", Title = "aaa", Runtime = 25, Rating = 2.5 });
            session.Store(new Movie { Id = "movies/2", Title = "mmm", Runtime = 120, Rating = 6.0 });
            session.Store(new Movie { Id = "movies/3", Title = "zzz", Runtime = 320, Rating = 9.5 });
            session.SaveChanges();
        }

        store.ExecuteIndex(new Movies_Showcase());
        Indexes.WaitForIndexing(store);

        return store;
    }

    private static string[] Query(IDocumentStore store, string where, params (string Name, object Value)[] parameters)
    {
        using var session = store.OpenSession();
        var query = session.Advanced.RawQuery<Movie>($"from index 'Movies/Showcase' where {where}");
        foreach (var (name, value) in parameters)
            query = query.AddParameter(name, value);

        var results = query.Statistics(out QueryStatistics stats).ToList();

        Assert.Equal(results.Count, stats.TotalResults);

        return results.Select(x => x.Id).OrderBy(x => x).ToArray();
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NumericRangeWithLowAboveHighMatchesNothing(Options options)
    {
        using var store = SetupStore(options);

        Assert.Empty(Query(store, "Runtime between $lo and $hi", ("lo", 300), ("hi", 30)));
        Assert.Empty(Query(store, "Rating between $lo and $hi", ("lo", 9.0), ("hi", 3.0)));

        // `> x and < y` is translated into a between with both ends exclusive.
        Assert.Empty(Query(store, "Runtime > $lo and Runtime < $hi", ("lo", 300), ("hi", 30)));

        Assert.Empty(Query(store, "Runtime > $lo and Runtime < $hi", ("lo", 120), ("hi", 120)));

        // Sanity: ordinary ranges keep working.
        Assert.Equal(new[] { "movies/3" }, Query(store, "Runtime between $lo and $hi", ("lo", 300), ("hi", 400)));
        Assert.Equal(new[] { "movies/2" }, Query(store, "Runtime between $lo and $hi", ("lo", 120), ("hi", 120)));
        Assert.Equal(new[] { "movies/2", "movies/3" }, Query(store, "Rating between $lo and $hi", ("lo", 3.0), ("hi", 9.9)));
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void StringRangeWithLowAboveHighMatchesNothing(Options options)
    {
        using var store = SetupStore(options);

        Assert.Empty(Query(store, "Title between $lo and $hi", ("lo", "zzz"), ("hi", "aaa")));
        Assert.Empty(Query(store, "Title between $lo and $hi", ("lo", "zzz"), ("hi", "mmm")));
        Assert.Empty(Query(store, "Title > $lo and Title < $hi", ("lo", "mmm"), ("hi", "mmm")));

        // Sanity: ordinary ranges keep working.
        Assert.Equal(new[] { "movies/1", "movies/2" }, Query(store, "Title between $lo and $hi", ("lo", "aaa"), ("hi", "mmm")));
        Assert.Equal(new[] { "movies/2" }, Query(store, "Title between $lo and $hi", ("lo", "mmm"), ("hi", "mmm")));
    }
}
