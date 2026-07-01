using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace FastTests.Corax;

public class QueryPlanGraphTooltipTests : RavenTestBase
{
    public QueryPlanGraphTooltipTests(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Name { get; set; }
        public int Value { get; set; }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task PlanGraphDot_ExposesDataPropertiesAsTooltip(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 20; i++)
                await session.StoreAsync(new Item { Name = i % 2 == 0 ? "even" : "odd", Value = i });
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var rql = "from Items where Name = $n include timings()";
            await session.Advanced.AsyncRawQuery<Item>(rql)
                .AddParameter("n", "even")
                .WaitForNonStaleResults()
                .Timings(out var timings)
                .ToListAsync();

            var root = (QueryInspectionNode)timings.QueryPlan;
            var dot = root.Parameters["PlanGraphDot"];

            // Every node/edge that carries data_* facts must also expose them as a single-line tooltip.
            Assert.Contains("tooltip=\"", dot);

            // The tooltip uses the original (readable) key names; a node with more than one fact joins them
            // with the Graphviz line-break separator (an escaped "\\" left after EscapeAttr collapses the
            // newline to a space) — confirm a representative fact and the join are present.
            Assert.Contains("FieldName: Name", dot);
            Assert.Contains("\\\\ ", dot);

            // One line: a tooltip value must not contain a raw newline (EscapeAttr collapses them to spaces).
            foreach (var line in dot.Split('\n'))
            {
                int idx = line.IndexOf("tooltip=\"", System.StringComparison.Ordinal);
                if (idx < 0)
                    continue;
                int end = line.IndexOf('"', idx + "tooltip=\"".Length);
                Assert.True(end > idx, "tooltip attribute should be closed on the same line");
            }
        }
    }
}
