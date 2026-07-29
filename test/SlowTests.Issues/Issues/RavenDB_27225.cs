using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    // boost()/exact() are wrappers - they must never change which documents match.
    public class RavenDB_27225 : RavenTestBase
    {
        public RavenDB_27225(ITestOutputHelper output) : base(output)
        {
        }

        private class Doc
        {
            public string A { get; set; }
            public string B { get; set; }
            public string C { get; set; }
            public string E { get; set; }
        }

        private class Docs_ByFlags : AbstractIndexCreationTask<Doc>
        {
            public Docs_ByFlags()
            {
                Map = docs => from d in docs
                              select new { d.A, d.B, d.C, d.E };
            }
        }

        private static int Count(IDocumentSession session, string where, bool flag = true) =>
            session.Advanced.RawQuery<Doc>($"from index 'Docs/ByFlags' where {where}").AddParameter("flag", flag).ToList().Count;

        private void Seed(Options options, out Raven.Client.Documents.IDocumentStore store)
        {
            store = GetDocumentStore(options);

            using (var session = store.OpenSession())
            {
                session.Store(new Doc { E = "y", A = "y" });            // E and A
                session.Store(new Doc { E = "y", C = "y" });            // E and C
                session.Store(new Doc { E = "y", A = "y", C = "y" });    // E, A and C
                session.Store(new Doc { A = "y", B = "y" });            // A and B
                session.Store(new Doc { C = "y" });                     // C
                session.Store(new Doc { A = "y" });                     // A only
                session.SaveChanges();
            }

            new Docs_ByFlags().Execute(store);
            Indexes.WaitForIndexing(store);
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void BoostedDisjunctionUnderAndMustNotLoseRows(Options options)
        {
            Seed(options, out var store);
            using (store)
            using (var session = store.OpenSession())
            {
                // control: the same predicate without the wrapper
                Assert.Equal(3, Count(session, "E = \"y\" and (A = \"y\" or C = \"y\")"));

                // boost is pure scoring, so the count has to be identical
                Assert.Equal(3, Count(session, "E = \"y\" and boost(A = \"y\" or C = \"y\", 3)"));
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void BoostedConjunctionUnderOrMustNotAddRows(Options options)
        {
            Seed(options, out var store);
            using (store)
            using (var session = store.OpenSession())
            {
                Assert.Equal(4, Count(session, "(A = \"y\" and B = \"y\") or C = \"y\""));

                Assert.Equal(4, Count(session, "boost(A = \"y\" and B = \"y\", 2) or C = \"y\""));
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ExactWrapperMustNotLeakItsConnectiveEither(Options options)
        {
            Seed(options, out var store);
            using (store)
            using (var session = store.OpenSession())
            {
                // exact() is the same kind of wrapper as boost(); the fields here are not analysed, so it
                // cannot change matching on its own.
                Assert.Equal(3, Count(session, "E = \"y\" and exact(A = \"y\" or C = \"y\")"));
                Assert.Equal(4, Count(session, "exact(A = \"y\" and B = \"y\") or C = \"y\""));
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void WhenWrapperMustNotLeakItsConnectiveEither(Options options)
        {
            Seed(options, out var store);
            using (store)
            using (var session = store.OpenSession())
            {
                // a satisfied when() condition keeps the predicate as-is, so the counts are the unwrapped ones
                Assert.Equal(3, Count(session, "E = \"y\" and when($flag = true, A = \"y\" or C = \"y\")"));
                Assert.Equal(4, Count(session, "when($flag = true, A = \"y\" and B = \"y\") or C = \"y\""));
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void WhenGuardOffUnderOrMustNotMatchEverything(Options options)
        {
            Seed(options, out var store);
            using (store)
            using (var session = store.OpenSession())
            {
                // an off guard drops the predicate, so what is left is the other side of the connective - and the
                // grouped operands must collapse to the identity of the ENCLOSING operator, not of their own
                const string underOr = "C = \"y\" or when($flag = true, A = \"y\" and B = \"y\")";
                Assert.Equal(3, Count(session, underOr, flag: false));   // must not become the whole index
                Assert.Equal(4, Count(session, underOr, flag: true));

                const string underAnd = "E = \"y\" and when($flag = true, A = \"y\" or C = \"y\")";
                Assert.Equal(3, Count(session, underAnd, flag: false));  // must not become nothing
                Assert.Equal(3, Count(session, underAnd, flag: true));
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ExactMustReachOperandsThatAreAlreadyGrouped(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                session.Store(new Doc { A = "y", B = "YY" });
                session.Store(new Doc { A = "y", B = "yy" });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                // auto index: exact() picks the non-analyzed field variant, so only the exact casing may match. The
                // parenthesised operand arrives as a group, and exact() has to reach the leaves under it - marking
                // only the group leaves them resolving against the analyzed (lowercased) field, which also matches "yy".
                var results = session.Advanced
                    .RawQuery<Doc>("from Docs where exact(A = $a and (B = $b or C = $b))")
                    .AddParameter("a", "y")
                    .AddParameter("b", "YY")
                    .ToList();

                Assert.Equal(1, results.Count);
                Assert.Equal("YY", results[0].B);
            }
        }

        private class Tagged
        {
            public string Id { get; set; }
            public string[] Tags { get; set; }
        }

        private class Tagged_ByTags : AbstractIndexCreationTask<Tagged>
        {
            public Tagged_ByTags()
            {
                Map = docs => from d in docs
                              select new { d.Tags };
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void BoostFactorMustSurviveAnOperandThatIsGrouped(Options options)
        {
            const int perGroup = 3;
            using var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                for (int i = 1; i <= perGroup; i++)
                {
                    session.Store(new Tagged { Tags = ["drama", "dx"] }, $"tagged/hi/{i}");
                    session.Store(new Tagged { Tags = ["action", "ax"] }, $"tagged/lo/{i}");
                }

                session.SaveChanges();
            }

            new Tagged_ByTags().Execute(store);
            Indexes.WaitForIndexing(store);

            // Both arms are conjunctions under an OR, so both arrive as groups. The factor rides on the clause's own
            // bindings and a group evaluates none, so it has to reach the leaves - otherwise both arms weigh the same
            // and flipping the weights cannot flip the order.
            const string q = "from index 'Tagged/ByTags' " +
                             "where boost(exact(Tags = $h1 and Tags = $h2), $wHi) or boost(exact(Tags = $l1 and Tags = $l2), $wLo) " +
                             "order by score()";

            using (var session = store.OpenSession())
            {
                var hiBoosted = session.Advanced.RawQuery<Tagged>(q)
                    .AddParameter("h1", "drama").AddParameter("h2", "dx")
                    .AddParameter("l1", "action").AddParameter("l2", "ax")
                    .AddParameter("wHi", 10).AddParameter("wLo", 2)
                    .ToList();
                AssertGroupLeads(hiBoosted, "tagged/hi/", "tagged/lo/", perGroup);

                var loBoosted = session.Advanced.RawQuery<Tagged>(q)
                    .AddParameter("h1", "drama").AddParameter("h2", "dx")
                    .AddParameter("l1", "action").AddParameter("l2", "ax")
                    .AddParameter("wHi", 2).AddParameter("wLo", 10)
                    .ToList();
                AssertGroupLeads(loBoosted, "tagged/lo/", "tagged/hi/", perGroup);
            }
        }

        private static void AssertGroupLeads(System.Collections.Generic.List<Tagged> results, string leading, string trailing, int perGroup)
        {
            Assert.Equal(2 * perGroup, results.Count);

            int lastLeading = results.FindLastIndex(r => r.Id.StartsWith(leading));
            int firstTrailing = results.FindIndex(r => r.Id.StartsWith(trailing));
            Assert.True(lastLeading >= 0 && firstTrailing >= 0, "both groups must be present");
            Assert.True(lastLeading < firstTrailing,
                $"expected every '{leading}' doc ahead of every '{trailing}' doc, got: {string.Join(", ", results.Select(r => r.Id))}");
        }
    }
}
