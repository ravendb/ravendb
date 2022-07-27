using FastTests;
using FastTests.Voron.FixedSize;
using FastTests.Voron.Trees;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Voron.Trees
{
    public class FreeSpaceStressTests : NoDisposalNoOutputNeeded
    {
        public FreeSpaceStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(400000, 60, 2)] // originally set in the test
        [InlineDataWithRandomSeed(400000, 60)]
        [InlineDataWithRandomSeed(-1, -1)] // random 'maxPageNumber' and 'numberOfFreedPages'
        public void FreeSpaceHandlingShouldNotReturnPagesThatAreAlreadyAllocated(int maxPageNumber,
            int numberOfFreedPages, int seed)
        {
            using (var test = new FreeSpaceTest(Output))
            {
                test.FreeSpaceHandlingShouldNotReturnPagesThatAreAlreadyAllocated(maxPageNumber, numberOfFreedPages, seed);
            }
        }
    }
}
