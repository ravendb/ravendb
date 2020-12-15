using FastTests;
using SlowTests.Utils;
using SlowTests.Voron;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Voron
{
    public class LargeFixedSizeTreesStressCases : NoDisposalNoOutputNeeded
    {
        public LargeFixedSizeTreesStressCases(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineDataWithRandomSeed(94000)]
        [InlineDataWithRandomSeed(300000)]
        public void CanDeleteRange_TryToFindABranchNextToLeaf(int count, int seed)
        {
            using (var test = new LargeFixedSizeTrees(Output))
            {
                test.CanDeleteRange_TryToFindABranchNextToLeaf(count, seed);
            }
        }

        [Theory]
        [InlineDataWithRandomSeed(1000000)]
        [InlineDataWithRandomSeed(2000000)]
        [InlineData(2000000, 1877749431)]// reproduced a bug, do not remove
        [InlineData(2000000, 1432104715)]// reproduced a bug, do not remove
        public void CanDeleteRange_RandomRanges(int count, int seed)
        {
            using (var test = new LargeFixedSizeTrees(Output))
            {
                test.CanDeleteRange_RandomRanges(count, seed);
            }
        }

        [Theory]
        [InlineDataWithRandomSeed(300000)]
        public void CanDeleteRange_RandomRanges_WithGaps(int count, int seed)
        {
            using (var test = new LargeFixedSizeTrees(Output))
            {
                test.CanDeleteRange_RandomRanges_WithGaps(count, seed);
            }
        }
    }
}
