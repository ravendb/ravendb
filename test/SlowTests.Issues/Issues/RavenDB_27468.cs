using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27468 : RavenTestBase
{
    public RavenDB_27468(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OrderByAlphanumericMustHandleMultiByteTerms(Options options)
    {
        const int documents = 600;

        using var store = GetDocumentStore(options);

        using (var bulk = store.BulkInsert())
        {
            // identical titles force every comparison to walk to the end of the term
            for (int i = 0; i < documents; i++)
                bulk.Store(new Movie { Language = "he", Title = "סרט ההמשך הגדול" });
        }

        new Movies_ByTitle().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var titles = session.Advanced
                .RawQuery<Movie>("from index 'Movies/ByTitle' where Language = 'he' order by Title as alphanumeric")
                .ToList();

            Assert.Equal(documents, titles.Count);
        }
    }

    private class Movie
    {
        public string Language { get; set; }

        public string Title { get; set; }
    }

    private class Movies_ByTitle : AbstractIndexCreationTask<Movie>
    {
        public Movies_ByTitle()
        {
            Map = movies => from m in movies
                            select new { m.Language, m.Title };

            Index(x => x.Title, FieldIndexing.Search);
        }
    }
}
