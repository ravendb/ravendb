using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27218 : RavenTestBase
    {
        public RavenDB_27218(ITestOutputHelper output) : base(output)
        {
        }

        private class Movie
        {
            public string Genre { get; set; }
            public DateTime? ReleaseDate { get; set; }
        }

        private class Movies_ByGenreAndReleaseDate : AbstractIndexCreationTask<Movie>
        {
            public Movies_ByGenreAndReleaseDate()
            {
                Map = movies => from m in movies
                                select new
                                {
                                    m.Genre,
                                    m.ReleaseDate
                                };

                CompoundField("Genre", "ReleaseDate");
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void EqualityOnNullMustMatchWhenACompoundFieldCoversBothClauses(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenSession())
            {
                session.Store(new Movie { Genre = "Action", ReleaseDate = null });
                session.Store(new Movie { Genre = "Action", ReleaseDate = null });
                session.Store(new Movie { Genre = "Action", ReleaseDate = new DateTime(2020, 1, 1) });
                session.Store(new Movie { Genre = "Drama", ReleaseDate = null });
                session.SaveChanges();
            }

            new Movies_ByGenreAndReleaseDate().Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                // Two equality clauses that are both covered by the compound field (Genre, ReleaseDate).
                // The null value must still match the two Action documents that have no ReleaseDate.
                var both = session.Advanced
                    .RawQuery<Movie>("from index \"Movies/ByGenreAndReleaseDate\" where Genre = $g and ReleaseDate = null")
                    .AddParameter("g", "Action")
                    .ToList();

                Assert.Equal(2, both.Count);

                // Controls: each clause on its own already works.
                var byGenre = session.Advanced
                    .RawQuery<Movie>("from index \"Movies/ByGenreAndReleaseDate\" where Genre = $g")
                    .AddParameter("g", "Action")
                    .ToList();

                Assert.Equal(3, byGenre.Count);

                var byNull = session.Advanced
                    .RawQuery<Movie>("from index \"Movies/ByGenreAndReleaseDate\" where ReleaseDate = null")
                    .ToList();

                Assert.Equal(3, byNull.Count);
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void EqualityOnEmptyStringMustMatchWhenACompoundFieldCoversBothClauses(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenSession())
            {
                session.Store(new Movie { Genre = "", ReleaseDate = new DateTime(2020, 1, 1) });
                session.Store(new Movie { Genre = "", ReleaseDate = new DateTime(2020, 1, 1) });
                session.Store(new Movie { Genre = "Action", ReleaseDate = new DateTime(2020, 1, 1) });
                session.SaveChanges();
            }

            new Movies_ByGenreAndReleaseDate().Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                // The compound writer folds an empty string into the same "no bytes" component as null, so an
                // empty-string equality cannot be answered through the compound field either.
                var results = session.Advanced
                    .RawQuery<Movie>("from index \"Movies/ByGenreAndReleaseDate\" where Genre = $g and ReleaseDate = $d")
                    .AddParameter("g", "")
                    .AddParameter("d", new DateTime(2020, 1, 1))
                    .ToList();

                Assert.Equal(2, results.Count);
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void EqualityOnNullMustMatchEvenWhenAPlanWasCachedForANonNullParameter(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenSession())
            {
                session.Store(new Movie { Genre = "Action", ReleaseDate = null });
                session.Store(new Movie { Genre = "Action", ReleaseDate = null });
                session.Store(new Movie { Genre = "Action", ReleaseDate = new DateTime(2020, 1, 1) });
                session.SaveChanges();
            }

            new Movies_ByGenreAndReleaseDate().Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                const string rql = "from index \"Movies/ByGenreAndReleaseDate\" where Genre = $g and ReleaseDate = $d";

                // Warm the plan with a non-null parameter first: null and a short string share one plan-cache
                // entry, so whichever value runs first pins the execution strategy.
                var warm = session.Advanced
                    .RawQuery<Movie>(rql)
                    .AddParameter("g", "Action")
                    .AddParameter("d", new DateTime(2020, 1, 1))
                    .ToList();

                Assert.Equal(1, warm.Count);

                // Same query shape, now with null - the two Action documents without a ReleaseDate must still match.
                var withNull = session.Advanced
                    .RawQuery<Movie>(rql)
                    .AddParameter("g", "Action")
                    .AddParameter("d", (object)null)
                    .ToList();

                Assert.Equal(2, withNull.Count);
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void EqualityOnNullMustMatchWhenTheCompoundFieldDrivesASortedScan(Options options)
        {
            using var store = GetDocumentStore(options);

            // Every document has a ReleaseDate, so the sort field itself has no nulls - only the driving
            // equality (Genre) is null.
            using (var session = store.OpenSession())
            {
                session.Store(new Movie { Genre = null, ReleaseDate = new DateTime(2020, 1, 1) });
                session.Store(new Movie { Genre = null, ReleaseDate = new DateTime(2021, 1, 1) });
                session.Store(new Movie { Genre = "Action", ReleaseDate = new DateTime(2022, 1, 1) });
                session.SaveChanges();
            }

            new Movies_ByGenreAndReleaseDate().Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var byNullGenre = session.Advanced
                    .RawQuery<Movie>("from index \"Movies/ByGenreAndReleaseDate\" where Genre = null order by ReleaseDate")
                    .ToList();

                Assert.Equal(2, byNullGenre.Count);
            }
        }
    }
}
