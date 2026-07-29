using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27184 : RavenTestBase
    {
        public RavenDB_27184(ITestOutputHelper output) : base(output)
        {
        }

        private class Movie
        {
            public string Id { get; set; }
            public long Runtime { get; set; }
        }

        private class Movies_Showcase : AbstractIndexCreationTask<Movie>
        {
            public Movies_Showcase()
            {
                Map = movies => from m in movies
                                select new { m.Runtime };
            }
        }

        private IDocumentStore Seed(Options options)
        {
            var store = GetDocumentStore(options);
            store.ExecuteIndex(new Movies_Showcase());

            using (var session = store.OpenSession())
            {
                session.Store(new Movie { Id = "movies/1", Runtime = 25 });
                session.Store(new Movie { Id = "movies/2", Runtime = 120 });
                session.Store(new Movie { Id = "movies/3", Runtime = 320 });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            return store;
        }

        private static int Count(IDocumentSession session, string where) =>
            session.Advanced.RawQuery<Movie>($"from index 'Movies/Showcase' {where}").ToList().Count;

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void NotOfAStaticallyFalseOperandMustMatchEverything(Options options)
        {
            using var store = Seed(options);
            using var session = store.OpenSession();

            // inverted bounds, so the clause is folded to 'always false' instead of being scanned;
            // its negation has to match every document
            Assert.Equal(3, Count(session, "where true and not (Runtime between 300 and 30)"));
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void NotOfAStaticallyTrueOperandMustMatchNothing(Options options)
        {
            using var store = Seed(options);
            using var session = store.OpenSession();

            // the same fold the other way round: 'always true', whose negation must match nothing
            Assert.Equal(0, Count(session, "where true and (true and not true)"));
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void AStaticallyFalseOperandMustAnnihilateTheAnd(Options options)
        {
            using var store = Seed(options);
            using var session = store.OpenSession();

            // `not true` is false, and false AND anything is false - the other side must not leak through
            Assert.Equal(0, Count(session, "where true and (Runtime = 120 and not true)"));
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void NotOfAScannedOperandMustMatchEverything(Options options)
        {
            using var store = Seed(options);
            using var session = store.OpenSession();

            // control - valid bounds, so the clause is scanned rather than folded. It matches nothing
            // and the negation is applied correctly, which is what still passes today.
            Assert.Equal(3, Count(session, "where true and not (Runtime between 999999 and 1000000)"));
        }
    }
}
