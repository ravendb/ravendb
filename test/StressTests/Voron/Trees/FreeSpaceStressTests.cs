using System.Threading.Tasks;
using FastTests.Voron.FixedSize;
using FastTests.Voron.Trees;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Voron.Trees
{
    public class FreeSpaceStressTests : NoDisposalNoOutputNeeded
    {
        public FreeSpaceStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(400000, 60, 2)] // originally set in the test
        [InlineDataWithRandomSeed(400000, 60)]
        [InlineDataWithRandomSeed(-1, -1)] // random 'maxPageNumber' and 'numberOfFreedPages'
        public async Task FreeSpaceHandlingShouldNotReturnPagesThatAreAlreadyAllocated(int maxPageNumber,
            int numberOfFreedPages, int seed)
        {
            await using (var test = new FreeSpaceTest(Output))
            {
                test.FreeSpaceHandlingShouldNotReturnPagesThatAreAlreadyAllocated(maxPageNumber, numberOfFreedPages, seed);
            }
        }
    }
}
