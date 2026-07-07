using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Voron
{
    public class RavenDB_26746 : NoDisposalNoOutputNeeded
    {
        public RavenDB_26746(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(15, 4)]
        [InlineData(15, 5)]
        [InlineData(15, 10)]
        [InlineData(50, 3)] // on unfixed code this seed failed with the search-misroute symptom (out-of-order separators)
        public async Task Rebalancing_must_not_corrupt_tree_when_separator_readd_splits_ancestors(int maxCycles, int seed)
        {
            await using (var test = new SlowTests.Voron.Issues.RavenDB_26746(Output))
            {
                test.Rebalancing_must_not_corrupt_tree_when_separator_readd_splits_ancestors(maxCycles, seed);
            }
        }
    }
}
