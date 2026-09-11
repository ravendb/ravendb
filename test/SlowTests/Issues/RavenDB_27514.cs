using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27514 : RavenTestBase
{
    public RavenDB_27514(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void PagingAnOrderByOnAListFieldReturnsTheCorrectPage(Options options)
    {
        using var store = GetDocumentStore(options);

        Seed(store);

        store.ExecuteIndex(new Movies_ByTitle());
        Indexes.WaitForIndexing(store);

        Assert.Equal(new[] { "movies/1", "movies/2" }, Page(store, "from index 'Movies/ByTitle' order by Title as long desc limit 2"));
        Assert.Equal(new[] { "movies/2", "movies/1" }, Page(store, "from index 'Movies/ByTitle' order by Title as long asc limit 2"));

        // the cap is computed from skip plus take, so the second page was cut short as well
        Assert.Equal(new[] { "movies/2" }, Page(store, "from index 'Movies/ByTitle' order by Title as long desc limit 1, 1"));
        Assert.Equal(new[] { "movies/1" }, Page(store, "from index 'Movies/ByTitle' order by Title as long asc limit 1, 1"));

        // a page wide enough to cover every term was always correct
        Assert.Equal(new[] { "movies/1", "movies/2" }, Page(store, "from index 'Movies/ByTitle' order by Title as long desc limit 6"));
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void PagingADynamicOrderByOnAListFieldReturnsTheCorrectPage(Options options)
    {
        using var store = GetDocumentStore(options);

        Seed(store);

        Assert.Equal(new[] { "movies/1", "movies/2" }, Page(store, "from 'Movies' order by Title as long desc limit 2"));
        Assert.Equal(new[] { "movies/2", "movies/1" }, Page(store, "from 'Movies' order by Title as long asc limit 2"));

        // the same cap applies to a string sort field
        Assert.Equal(new[] { "movies/1", "movies/2" }, Page(store, "from 'Movies' order by Tags desc limit 2"));
        Assert.Equal(new[] { "movies/2", "movies/1" }, Page(store, "from 'Movies' order by Tags asc limit 2"));
    }

    private static void Seed(IDocumentStore store)
    {
        using var session = store.OpenSession();

        session.Store(new Movie
        {
            Id = "movies/1",
            Title = new object[] { 10L, 20L, 30L, 40L, 50L },
            Tags = new[] { "a", "b", "c", "d", "e" }
        });

        session.Store(new Movie { Id = "movies/2", Title = new object[] { 5L }, Tags = new[] { "0" } });
        session.SaveChanges();
    }

    private static string[] Page(IDocumentStore store, string query)
    {
        using var session = store.OpenSession();

        return session.Advanced
            .RawQuery<Movie>(query)
            .WaitForNonStaleResults()
            .ToList()
            .Select(x => x.Id)
            .ToArray();
    }

    private sealed class Movie
    {
        public string Id { get; set; }

        public object Title { get; set; }

        public string[] Tags { get; set; }
    }

    private sealed class Movies_ByTitle : AbstractIndexCreationTask<Movie>
    {
        public Movies_ByTitle()
        {
            Map = movies => from movie in movies
                            select new { movie.Title };
        }

        public override string IndexName => "Movies/ByTitle";
    }
}
