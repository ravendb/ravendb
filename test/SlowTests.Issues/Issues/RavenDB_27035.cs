using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27035 : RavenTestBase
{
    public RavenDB_27035(ITestOutputHelper output) : base(output)
    {
    }

    private class Movie
    {
        public string Id { get; set; }

        public object Title { get; set; }

        public int Year { get; set; }
    }

    private class Movies_ByTitle : AbstractIndexCreationTask<Movie>
    {
        public Movies_ByTitle()
        {
            Map = movies => from m in movies
                            select new
                            {
                                m.Title
                            };
        }
    }

    // the second field keeps a document without Title in the index, so Title lands in the non-existing posting list
    private class Movies_ByTitleAndYear : AbstractIndexCreationTask<Movie>
    {
        public Movies_ByTitleAndYear()
        {
            Map = movies => from m in movies
                            select new
                            {
                                m.Title,
                                m.Year
                            };
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OrderByShouldNotDropDocumentsWhenFieldHasTimeAndStringValues(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Movie { Id = "movies/1", Title = "Alpha" });
                session.Store(new Movie { Id = "movies/2", Title = "Beta" });
                session.Store(new Movie { Id = "movies/3", Title = "Gamma" });
                session.Store(new Movie { Id = "movies/4", Title = "Zulu" });
                session.Store(new Movie { Id = "movies/5", Title = "01:02:03" }); // indexed as a TimeSpan, marks Title as a time field
                session.SaveChanges();
            }

            store.ExecuteIndex(new Movies_ByTitle());
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                Assert.Equal(5, session.Advanced.RawQuery<Movie>("from index 'Movies/ByTitle'").ToList().Count);
                Assert.Equal(5, session.Advanced.RawQuery<Movie>("from index 'Movies/ByTitle' order by Title").ToList().Count);
                Assert.Equal(5, session.Advanced.RawQuery<Movie>("from index 'Movies/ByTitle' order by Title desc").ToList().Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OrderByAsLongShouldNotDropDocumentsWhenFieldHasNumericAndStringValues(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Movie { Id = "movies/1", Title = "Alpha" });
                session.Store(new Movie { Id = "movies/2", Title = "Beta" });
                session.Store(new Movie { Id = "movies/3", Title = "Gamma" });
                session.Store(new Movie { Id = "movies/4", Title = "Zulu" });
                session.Store(new Movie { Id = "movies/5", Title = 300L });
                session.SaveChanges();
            }

            store.ExecuteIndex(new Movies_ByTitle());
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                Assert.Equal(5, session.Advanced.RawQuery<Movie>("from index 'Movies/ByTitle' order by Title as long").ToList().Count);
                Assert.Equal(5, session.Advanced.RawQuery<Movie>("from index 'Movies/ByTitle' order by Title as double").ToList().Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DirectScanIsStillUsedWhenTheSortFieldIsHomogeneous(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 10; i++)
                    session.Store(new Movie { Id = $"movies/{i}", Title = (long)i });

                session.SaveChanges();
            }

            store.ExecuteIndex(new Movies_ByTitle());
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                QueryTimings timings = null;

                var results = session.Advanced.RawQuery<Movie>("from index 'Movies/ByTitle' order by Title as long include timings()")
                    .Timings(out timings)
                    .ToList();

                Assert.Equal(10, results.Count);

                var plan = Assert.IsType<QueryInspectionNode>(timings.QueryPlan);

                // the numeric tree covers every entry, so the scan drives the query and the sort is elided
                Assert.True(PlanContains(plan, "DirectScan"), PlanOperations(plan));
                Assert.False(PlanContains(plan, "SortingMatch"), PlanOperations(plan));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DirectScanIsStillUsedWhenTheSortFieldIsAllStrings(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 10; i++)
                    session.Store(new Movie { Id = $"movies/{i}", Title = $"movie-{i}" });

                session.SaveChanges();
            }

            store.ExecuteIndex(new Movies_ByTitle());
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                QueryTimings timings = null;

                var results = session.Advanced.RawQuery<Movie>("from index 'Movies/ByTitle' order by Title include timings()")
                    .Timings(out timings)
                    .ToList();

                Assert.Equal(10, results.Count);

                var plan = Assert.IsType<QueryInspectionNode>(timings.QueryPlan);

                Assert.True(PlanContains(plan, "DirectScan"), PlanOperations(plan));
                Assert.False(PlanContains(plan, "SortingMatch"), PlanOperations(plan));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void OneValueOfAnotherTypeIsEnoughToDisqualifyTheScan(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            SetupNullAndMissingValues(store, extraStringTitle: true);

            using (var session = store.OpenSession())
            {
                QueryTimings timings = null;

                var results = session.Advanced.RawQuery<Movie>("from index 'Movies/ByTitleAndYear' order by Title as long include timings()")
                    .Timings(out timings)
                    .ToList();

                Assert.Equal(5, results.Count);

                var plan = Assert.IsType<QueryInspectionNode>(timings.QueryPlan);

                Assert.True(PlanContains(plan, "SortingMatch"), PlanOperations(plan));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void NullAndMissingValuesAloneDoNotChangeTheResultSet(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            SetupNullAndMissingValues(store, extraStringTitle: false);

            using (var session = store.OpenSession())
            {
                var results = session.Advanced.RawQuery<Movie>("from index 'Movies/ByTitleAndYear' order by Title as long").ToList();

                Assert.Equal(4, results.Count);
            }
        }
    }

    private void SetupNullAndMissingValues(IDocumentStore store, bool extraStringTitle)
    {
        using (var session = store.OpenSession())
        {
            session.Store(new Movie { Id = "movies/1", Title = 10L, Year = 2001 });
            session.Store(new Movie { Id = "movies/2", Title = 20L, Year = 2002 });
            session.Store(new Movie { Id = "movies/3", Title = null, Year = 2003 });

            if (extraStringTitle)
                session.Store(new Movie { Id = "movies/5", Title = "Alpha", Year = 2005 });

            session.SaveChanges();
        }

        // no Title field at all - it lands in the field's non-existing posting list
        using (var bulk = store.BulkInsert())
            bulk.Store(new { Year = 2004 }, "movies/4", new MetadataAsDictionary { [Constants.Documents.Metadata.Collection] = "Movies" });

        store.ExecuteIndex(new Movies_ByTitleAndYear());
        Indexes.WaitForIndexing(store);
    }

    // after the fix a mixed-type field falls back to the bitmap pipeline, so a page has to come out of the sort, not the scan
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void PagingANumericOrderByOnAMixedTypeFieldReturnsTheCorrectPage(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Movie { Id = "movies/10", Title = 10L });
                session.Store(new Movie { Id = "movies/20", Title = 20L });
                session.Store(new Movie { Id = "movies/30", Title = 30L });
                session.Store(new Movie { Id = "movies/40", Title = 40L });
                session.Store(new Movie { Id = "movies/str", Title = "Alpha" });
                session.SaveChanges();
            }

            store.ExecuteIndex(new Movies_ByTitle());
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var page = session.Advanced.RawQuery<Movie>("from index 'Movies/ByTitle' order by Title as long desc limit 2")
                    .ToList();

                Assert.Equal(new[] { "movies/40", "movies/30" }, page.Select(x => x.Id).ToArray());
            }
        }
    }

    // terms sharing 160 bytes are equal in the bytes the textual sort key keeps, so a page must not be picked by them
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void PagingATextualOrderByOnCollidingTermsReturnsTheCorrectPage(Options options)
    {
        var prefix = new string('a', 160);

        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                for (var i = 1; i <= 4; i++)
                    session.Store(new Movie { Id = $"movies/{i}", Title = prefix + i.ToString("0000") });

                session.Store(new Movie { Id = "movies/time", Title = "01:02:03" });
                session.SaveChanges();
            }

            store.ExecuteIndex(new Movies_ByTitle());
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var page = session.Advanced.RawQuery<Movie>("from index 'Movies/ByTitle' order by Title as string desc limit 2")
                    .ToList();

                Assert.Equal(new[] { "movies/4", "movies/3" }, page.Select(x => x.Id).ToArray());
            }
        }
    }

    private static bool PlanContains(QueryInspectionNode node, string operation)
    {
        if (node.Operation != null && node.Operation.Contains(operation, StringComparison.OrdinalIgnoreCase))
            return true;

        if (node.Children == null)
            return false;

        foreach (var child in node.Children)
        {
            if (PlanContains(child, operation))
                return true;
        }

        return false;
    }

    private static string PlanOperations(QueryInspectionNode node)
    {
        var operations = new List<string>();
        Collect(node, operations);
        return string.Join(" -> ", operations);

        static void Collect(QueryInspectionNode current, List<string> into)
        {
            into.Add(current.Operation);

            if (current.Children == null)
                return;

            foreach (var child in current.Children)
                Collect(child, into);
        }
    }
}
