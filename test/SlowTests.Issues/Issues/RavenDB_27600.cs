using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27600 : RavenTestBase
{
    public RavenDB_27600(ITestOutputHelper output) : base(output)
    {
    }

    private class Show
    {
        public string Id { get; set; }
        public string A { get; set; }
    }

    private class Shows_ByA : AbstractIndexCreationTask<Show>
    {
        public Shows_ByA()
        {
            Map = shows => from s in shows select new { D = s.A };
        }
    }

    private class Shows_ByA_NullsLargest : AbstractIndexCreationTask<Show>
    {
        public Shows_ByA_NullsLargest()
        {
            Map = shows => from s in shows select new { D = s.A };
            Configuration["Indexing.Querying.Corax.NullsSortMode"] = "NullsLargest";
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ExistsOnSortFieldWithDescendingOrderPutsNullsLast(Options options)
    {
        using var store = GetDocumentStore(options);
        new Shows_ByA().Execute(store);
        using (var session = store.OpenSession())
        {
            session.Store(new Show { A = "b" }, "shows/1");
            session.Store(new Show { A = null }, "shows/2");
            session.Store(new Show { A = "c" }, "shows/3");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var ids = session.Advanced.RawQuery<Show>("from index 'Shows/ByA' where exists(D) order by D desc").ToList().Select(x => x.Id);
            Assert.Equal(new[] { "shows/3", "shows/1", "shows/2" }, ids);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void ExistsOnSortFieldFollowsNullsOrdering()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        new Shows_ByA().Execute(store);
        new Shows_ByA_NullsLargest().Execute(store);
        using (var session = store.OpenSession())
        {
            session.Store(new Show { A = "b" }, "shows/1");
            session.Store(new Show { A = null }, "shows/2");
            session.Store(new Show { A = "c" }, "shows/3");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var nullsLast = session.Advanced.RawQuery<Show>("from index 'Shows/ByA' where exists(D) order by D nulls last").ToList().Select(x => x.Id);
            Assert.Equal(new[] { "shows/1", "shows/3", "shows/2" }, nullsLast);

            var nullsLargest = session.Advanced.RawQuery<Show>("from index 'Shows/ByA/NullsLargest' where exists(D) order by D").ToList().Select(x => x.Id);
            Assert.Equal(new[] { "shows/1", "shows/3", "shows/2" }, nullsLargest);
        }
    }
}
