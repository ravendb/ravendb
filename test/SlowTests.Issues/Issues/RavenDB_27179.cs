using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

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
        public void NegatedStartsWithAndEndsWithInAndMustKeepDocumentsMissingTheField(Options options)
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
                // Nothing matches "zzz", so the negation has to keep every document - including the two that have no
                // Tagline field at all, which is what was being dropped. The `exists(Title) and ...` form is what
                // routes the query through the negated-AND code path in Corax.
                Assert.Equal(4, Count(session, "not (startsWith(Tagline, $p))", "zzz"));
                Assert.Equal(4, Count(session, "not (endsWith(Tagline, $p))", "zzz"));

                // ...and it still has to drop what it does match, otherwise a negation that silently degraded into
                // 'match all' would satisfy the two assertions above.
                Assert.Equal(3, Count(session, "not (startsWith(Tagline, $p))", "hello")); // drops 'hello world'
                Assert.Equal(3, Count(session, "not (endsWith(Tagline, $p))", "one"));     // drops 'another one'
            }

            static int Count(IDocumentSession session, string negatedClause, string pattern) =>
                session.Advanced
                    .RawQuery<Movie>($"from index \"Movies/ByTagline\" where exists(Title) and {negatedClause}")
                    .AddParameter("p", pattern)
                    .ToList()
                    .Count;
        }
    }
}
