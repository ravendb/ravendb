using FastTests;
using SlowTests.Utils;
using SlowTests.Voron.Bugs;
using Xunit;

namespace StressTests.Voron.Bugs
{
    public class DataInconsistencyReproStressTests : NoDisposalNeeded
    {
        [Theory]
        [InlineDataWithRandomSeed(1000, 50000)]
        public void FaultyOverflowPagesHandling_CannotModifyReadOnlyPages(int initialNumberOfDocs,
            int numberOfModifications, int seed)
        {
            using (var test = new DataInconsistencyRepro(Output))
            {
                test.FaultyOverflowPagesHandling_CannotModifyReadOnlyPages(initialNumberOfDocs, numberOfModifications, seed);
            }
        }
    }
}