using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Regression suite for nested boolean groups in the Corax query planner: queries where a sub-clause of an
/// OrGroup or AndGroup is itself a group with non-leaf children — the path that falls through to
/// <c>ResolveClause</c>'s recursive bitmap-collapse instead of staying in the IL slot pipeline.
/// Each test asserts result correctness only.
/// </summary>
public class CompiledQueryNestedGroupTests : RavenTestBase
{
    public CompiledQueryNestedGroupTests(Xunit.ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NestedAndInsideOr_TwoConjuncts()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            // Layout: 100 docs, Category cycles a/b/c/d, Priority 1..5
            // - cat='a' AND pri=1 → docs at i % 4 == 0 AND i % 5 == 0 → i ∈ {0,20,40,60,80} = 5
            // - cat='b' AND pri=2 → i % 4 == 1 AND i % 5 == 1 → i ∈ {1,21,41,61,81} = 5
            for (int i = 0; i < 100; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i:D5}",
                    Category = "a b c d".Split(' ')[i % 4],
                    Priority = (i % 5) + 1
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where (Category = 'a' and Priority = 1) or (Category = 'b' and Priority = 2)")
                .ToListAsync();

            Assert.Equal(10, results.Count);
            Assert.All(results, r =>
                Assert.True((r.Category == "a" && r.Priority == 1) || (r.Category == "b" && r.Priority == 2),
                    $"Unexpected match: Category={r.Category}, Priority={r.Priority}"));
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NestedAndInsideOr_ThreeConjuncts()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 120; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i:D5}",
                    Category = "a b c d".Split(' ')[i % 4],
                    Priority = (i % 5) + 1,
                    Status = i % 2 == 0 ? "active" : "inactive"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where " +
                "(Category = 'a' and Priority = 1) or " +
                "(Category = 'b' and Priority = 2) or " +
                "(Category = 'c' and Status = 'active')")
                .ToListAsync();

            Assert.All(results, r =>
                Assert.True(
                    (r.Category == "a" && r.Priority == 1) ||
                    (r.Category == "b" && r.Priority == 2) ||
                    (r.Category == "c" && r.Status == "active"),
                    $"Unexpected match: Category={r.Category}, Priority={r.Priority}, Status={r.Status}"));

            // Verify count is non-zero and matches expected predicate over the fixture
            var expected = 0;
            for (int i = 0; i < 120; i++)
            {
                var cat = "a b c d".Split(' ')[i % 4];
                var pri = (i % 5) + 1;
                var sta = i % 2 == 0 ? "active" : "inactive";
                if ((cat == "a" && pri == 1) || (cat == "b" && pri == 2) || (cat == "c" && sta == "active"))
                    expected++;
            }
            Assert.Equal(expected, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NestedOrInsideAnd_LeafAndAndGroups()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 200; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i:D5}",
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Tag = "alpha beta gamma delta".Split(' ')[i % 4],
                    Priority = (i % 6) + 1
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenAsyncSession())
        {
            // Active items that are EITHER (alpha,pri=1) OR (beta,pri=2)
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Status = 'active' and " +
                "((Tag = 'alpha' and Priority = 1) or (Tag = 'beta' and Priority = 2))")
                .ToListAsync();

            Assert.All(results, r =>
            {
                Assert.Equal("active", r.Status);
                Assert.True(
                    (r.Tag == "alpha" && r.Priority == 1) ||
                    (r.Tag == "beta" && r.Priority == 2),
                    $"Unexpected match: Tag={r.Tag}, Priority={r.Priority}");
            });

            var expected = 0;
            for (int i = 0; i < 200; i++)
            {
                var sta = i % 2 == 0 ? "active" : "inactive";
                var tag = "alpha beta gamma delta".Split(' ')[i % 4];
                var pri = (i % 6) + 1;
                if (sta == "active" && ((tag == "alpha" && pri == 1) || (tag == "beta" && pri == 2)))
                    expected++;
            }
            Assert.Equal(expected, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NestedTripleDepth_AndWithOrWithAnd()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 150; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i:D5}",
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Category = "x y z".Split(' ')[i % 3],
                    Tag = "hot cold warm".Split(' ')[i % 3],
                    Priority = (i % 5) + 1
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenAsyncSession())
        {
            // Three-level: active AND (cat=x OR (tag=hot AND priority=1))
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Status = 'active' and " +
                "(Category = 'x' or (Tag = 'hot' and Priority = 1))")
                .ToListAsync();

            Assert.All(results, r =>
            {
                Assert.Equal("active", r.Status);
                Assert.True(
                    r.Category == "x" || (r.Tag == "hot" && r.Priority == 1),
                    $"Unexpected match: Category={r.Category}, Tag={r.Tag}, Priority={r.Priority}");
            });

            var expected = 0;
            for (int i = 0; i < 150; i++)
            {
                var sta = i % 2 == 0 ? "active" : "inactive";
                var cat = "x y z".Split(' ')[i % 3];
                var tag = "hot cold warm".Split(' ')[i % 3];
                var pri = (i % 5) + 1;
                if (sta == "active" && (cat == "x" || (tag == "hot" && pri == 1)))
                    expected++;
            }
            Assert.Equal(expected, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NestedWithNegation_AndNotInsideOrGroup()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 100; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i:D5}",
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Category = "a b c d".Split(' ')[i % 4],
                    Priority = (i % 5) + 1
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenAsyncSession())
        {
            // active AND ((category=a AND priority != 1) OR (category=b AND priority = 2))
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Status = 'active' and " +
                "((Category = 'a' and Priority != 1) or (Category = 'b' and Priority = 2))")
                .ToListAsync();

            Assert.All(results, r =>
            {
                Assert.Equal("active", r.Status);
                Assert.True(
                    (r.Category == "a" && r.Priority != 1) ||
                    (r.Category == "b" && r.Priority == 2),
                    $"Unexpected match: Category={r.Category}, Priority={r.Priority}");
            });

            var expected = 0;
            for (int i = 0; i < 100; i++)
            {
                var sta = i % 2 == 0 ? "active" : "inactive";
                var cat = "a b c d".Split(' ')[i % 4];
                var pri = (i % 5) + 1;
                if (sta == "active" && ((cat == "a" && pri != 1) || (cat == "b" && pri == 2)))
                    expected++;
            }
            Assert.Equal(expected, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NestedWithNegation_NotEqualsDirectInsideOrGroup()
    {
        // Probe for an asymmetry: NotEquals as a DIRECT sub-clause of a nested OrGroup
        // (not wrapped in an inner AndGroup). NotCanonicalize only marks top-level
        // OR-chain clauses with IsOrChainNotEquals=true; nested OrGroup sub-clauses
        // are not marked. If the OrGroup collapse path (or the IL pipeline's nested
        // OrGroup case in EmitAndPlan) doesn't handle the negation, the positive form
        // leaks through and the result set is wrong.
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 100; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i:D5}",
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Category = "a b c d".Split(' ')[i % 4],
                    Priority = (i % 5) + 1
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenAsyncSession())
        {
            // active AND (Priority != 1 OR Category = 'b')
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Status = 'active' and " +
                "(Priority != 1 or Category = 'b')")
                .ToListAsync();

            Assert.All(results, r =>
            {
                Assert.Equal("active", r.Status);
                Assert.True(
                    r.Priority != 1 || r.Category == "b",
                    $"Unexpected match: Category={r.Category}, Priority={r.Priority}");
            });

            var expected = 0;
            for (int i = 0; i < 100; i++)
            {
                var sta = i % 2 == 0 ? "active" : "inactive";
                var cat = "a b c d".Split(' ')[i % 4];
                var pri = (i % 5) + 1;
                if (sta == "active" && (pri != 1 || cat == "b"))
                    expected++;
            }
            Assert.Equal(expected, results.Count);
        }
    }

    private class TestDoc
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Status { get; set; }
        public string Category { get; set; }
        public string Tag { get; set; }
        public int Priority { get; set; }
    }
}
