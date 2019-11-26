using FastTests;
using SlowTests.Utils;
using SlowTests.Voron.Bugs;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Voron.Bugs
{
    public class DataInconsistencyReproStressTests : NoDisposalNoOutputNeeded
    {
        public DataInconsistencyReproStressTests(ITestOutputHelper output) : base(output)
        {
        }

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
