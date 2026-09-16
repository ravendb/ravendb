using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    // RavenDB-25281: limit-aware bitmap accumulation (CoraxIndexReadOperation sets CompiledQueryMatch.Limit
    // when there is no ORDER BY / DISTINCT / Filter and the client did not request statistics) used to be
    // applied by the seed fill itself: the fill stopped after Limit entries. For an AND chain that is wrong —
    // the seed feeds a narrowing clause, so truncating it to Limit drops entries that would have survived the
    // AND, leaving fewer than Limit results even though far more documents match.
    //
    // The fix routes fill/AND truncation through CompiledQueryMatch.OpLimit, which the compiled delegate only
    // arms once slot 0 stops being narrowed (no later AND/ANDNOT and no entry-scan gate). This test pins the
    // adversarial layout where the lowest-id members of *either* candidate clause fail the conjunction, so a
    // truncated seed yields zero results while the true answer has thousands.
    public class RavenDB_25281_SeedFillLimitTruncation : RavenTestBase
    {
        public RavenDB_25281_SeedFillLimitTruncation(ITestOutputHelper output) : base(output)
        {
        }

        private sealed class Item
        {
            public string Id { get; set; }
            public string FieldA { get; set; }
            public string FieldB { get; set; }
        }

        // A = 'a' on ids [0..14] ∪ [100..2099]; B = 'b' on ids [50..64] ∪ [100..2099].
        // A∧B = [100..2099] (2000 docs). A's 15 lowest ids (0..14) are not in B; B's 15 lowest ids (50..64)
        // are not in A — so whichever clause the planner seeds, its first 15 posting-list entries fail the
        // conjunction, and a limit-truncated seed produces an empty result.
        private const int DocCount = 2100;
        private const int ConjunctionStart = 100;

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void LimitAwareSeedMustNotTruncateBeforeAnd()
        {
            using IDocumentStore store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < DocCount; i++)
                {
                    bool inConjunction = i >= ConjunctionStart;
                    bool aOnly = i is >= 0 and <= 14;
                    bool bOnly = i is >= 50 and <= 64;

                    bulk.Store(new Item
                    {
                        FieldA = inConjunction || aOnly ? "a" : $"a-unique-{i}",
                        FieldB = inConjunction || bOnly ? "b" : $"b-unique-{i}"
                    });
                }
            }

            Indexes.WaitForIndexing(store);

            const int take = 15;

            using IDocumentSession session = store.OpenSession();
            // No .Statistics(out _) -> client sends SkipStatistics=true -> server arms limit-aware accumulation.
            // Two equalities, no ORDER BY -> bitmap pipeline with an AND behind the seed.
            List<Item> results = session.Query<Item>()
                .Where(x => x.FieldA == "a" && x.FieldB == "b")
                .Take(take)
                .ToList();

            // 2000 documents satisfy the conjunction, so a correct limit-aware read returns a full page.
            // Pre-fix the seed was truncated to the first 15 of its clause (all of which fail the other clause),
            // so the AND produced zero rows.
            Assert.Equal(take, results.Count);
        }
    }
}
