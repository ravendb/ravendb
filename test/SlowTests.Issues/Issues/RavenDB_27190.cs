using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27190 : RavenTestBase
    {
        public RavenDB_27190(ITestOutputHelper output) : base(output)
        {
        }

        private class Question
        {
            public string Tag { get; set; }
        }

        private class Questions_ByTag : AbstractIndexCreationTask<Question>
        {
            public Questions_ByTag()
            {
                Map = questions => from q in questions
                                   select new
                                   {
                                       q.Tag
                                   };
            }
        }

        private const int TagCount = 2000;

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void OrOfEqualsAndExistsMustNotCollapseToTheEqualsBranchWhenOrdered(Options options)
        {
            using var store = GetDocumentStore(options);

            // Enough documents that scanning the sort field's terms looks cheaper than a bitmap - that is what
            // makes the planner pick the direct scan, and only then does the OR degenerate.
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < TagCount; i++)
                    bulk.Store(new Question { Tag = $"tag-{i:D5}" });

                bulk.Store(new Question { Tag = "rest" }); // the single document the equality branch matches
            }

            new Questions_ByTag().Execute(store);
            Indexes.WaitForIndexing(store);

            const int expected = TagCount + 1;

            using (var session = store.OpenSession())
            {
                // `Tag = 'rest' or exists(Tag)` is satisfied by every document that has a Tag, so the ORDER BY
                // must not shrink the result to the single document matching the equality branch.
                var ordered = session.Advanced
                    .RawQuery<Question>("from index \"Questions/ByTag\" where (Tag = $p0 or exists(Tag)) order by Tag")
                    .AddParameter("p0", "rest")
                    .ToList();

                Assert.Equal(expected, ordered.Count);

                // Control: the same query without ORDER BY, and the exists() branch on its own.
                var unordered = session.Advanced
                    .RawQuery<Question>("from index \"Questions/ByTag\" where (Tag = $p0 or exists(Tag))")
                    .AddParameter("p0", "rest")
                    .ToList();

                Assert.Equal(expected, unordered.Count);

                var existsOnly = session.Advanced
                    .RawQuery<Question>("from index \"Questions/ByTag\" where exists(Tag) order by Tag")
                    .ToList();

                Assert.Equal(expected, existsOnly.Count);
            }
        }
    }
}
