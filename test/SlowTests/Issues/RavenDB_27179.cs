using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_27179 : RavenTestBase
    {
        public RavenDB_27179(ITestOutputHelper output) : base(output)
        {
        }

        private class Movie
        {
            public string Title { get; set; }
            public string Tagline { get; set; }
        }

        private class Movies_ByTagline : AbstractIndexCreationTask<Movie>
        {
            public Movies_ByTagline()
            {
                Map = movies => from m in movies
                                select new
                                {
                                    m.Title,
                                    m.Tagline
                                };
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void NegatedStartsWithInAndMustKeepDocumentsMissingTheField(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenSession())
            {
                session.Store(new Movie { Title = "a", Tagline = "hello world" }); // has tagline
                session.Store(new Movie { Title = "b" });                          // no tagline
                session.Store(new Movie { Title = "c", Tagline = "another one" }); // has tagline
                session.Store(new Movie { Title = "d" });                          // no tagline
                session.SaveChanges();
            }

            new Movies_ByTagline().Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                // No tagline starts with "zzz", so `not startsWith(Tagline, "zzz")` must keep every document,
                // including the two that have no Tagline field at all.
                // The `exists(Title) and ...` form is what triggers the negated-AND code path in Corax.
                var results = session.Advanced
                    .RawQuery<Movie>("from index \"Movies/ByTagline\" where exists(Title) and not (startsWith(Tagline, $p))")
                    .AddParameter("p", "zzz")
                    .ToList();

                Assert.Equal(4, results.Count);
            }
        }
    }
}
