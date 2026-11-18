using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    // A false when() guard drops its predicate, so what is left has to be the identity of the ENCLOSING operator -
    // and a WHERE that has ceased to exist filters nothing at all.
    public class RavenDB_27227 : RavenTestBase
    {
        public RavenDB_27227(ITestOutputHelper output) : base(output)
        {
        }

        private class Doc
        {
            public string Z { get; set; }
            public string A { get; set; }
            public string C { get; set; }
        }

        private class Docs_ByFlags : AbstractIndexCreationTask<Doc>
        {
            public Docs_ByFlags()
            {
                Map = docs => from d in docs
                              select new { d.Z, d.A, d.C };
            }
        }

        private const int AllDocs = 4;
        private const int ZDocs = 3;

        private static int Count(IDocumentSession session, string where, bool guard = false) =>
            session.Advanced.RawQuery<Doc>($"from index 'Docs/ByFlags' where {where}")
                .AddParameter("f", guard).ToList().Count;

        private IDocumentStore Seed(Options options)
        {
            var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Z = "y", A = "y" });
                session.Store(new Doc { Z = "y", C = "y" });
                session.Store(new Doc { Z = "y" });
                session.Store(new Doc { A = "y" });
                session.SaveChanges();
            }

            new Docs_ByFlags().Execute(store);
            Indexes.WaitForIndexing(store);
            return store;
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void RootLevelGuardOffLeavesNoFilter(Options options)
        {
            using var store = Seed(options);
            using var session = store.OpenSession();

            // the when() is the whole WHERE, so an off guard leaves nothing to filter on - the inner connective
            // must not decide the answer
            Assert.Equal(AllDocs, Count(session, "when($f = true, A = \"y\" or C = \"y\")"));
            Assert.Equal(AllDocs, Count(session, "when($f = true, A = \"y\" and C = \"y\")"));
            Assert.Equal(AllDocs, Count(session, "when($f = true, A = \"y\")"));

            // guard on - the predicate applies again
            Assert.Equal(3, Count(session, "when($f = true, A = \"y\" or C = \"y\")", guard: true));
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void GroupEmptiedByGuardsTakesItsParentsIdentity(Options options)
        {
            using var store = Seed(options);
            using var session = store.OpenSession();

            // every clause inside the parentheses is dropped, so the group is empty and only Z is left to filter on
            Assert.Equal(ZDocs, Count(session, "Z = \"y\" and (when($f = true, A = \"y\") or when($f = true, C = \"y\"))"));
            Assert.Equal(ZDocs, Count(session, "Z = \"y\" or (when($f = true, A = \"y\") and when($f = true, C = \"y\"))"));

            // an absent clause takes its negation with it
            Assert.Equal(ZDocs, Count(session, "Z = \"y\" and not (when($f = true, A = \"y\") or when($f = true, C = \"y\"))"));

            // guard on - the group filters again
            Assert.Equal(2, Count(session, "Z = \"y\" and (when($f = true, A = \"y\") or when($f = true, C = \"y\"))", guard: true));
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void GuardOffBesideASurvivingSiblingKeepsTheSibling(Options options)
        {
            using var store = Seed(options);
            using var session = store.OpenSession();

            // a live sibling remains, so the dropped clause takes the joining identity - these already worked and
            // must keep working
            Assert.Equal(ZDocs, Count(session, "Z = \"y\" and when($f = true, A = \"y\" or C = \"y\")"));
            Assert.Equal(ZDocs, Count(session, "Z = \"y\" or when($f = true, A = \"y\" and C = \"y\")"));
        }
    }
}
