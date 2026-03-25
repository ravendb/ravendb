using System.Threading.Tasks;
using SlowTests.Utils;
using SlowTests.Voron.Bugs;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Voron.Bugs
{
    public class DataInconsistencyReproStressTests : NoDisposalNoOutputNeeded
    {
        public DataInconsistencyReproStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed(1000, 50000)]
        public async Task FaultyOverflowPagesHandling_CannotModifyReadOnlyPages(int initialNumberOfDocs,
            int numberOfModifications, int seed)
        {
            await using (var test = new DataInconsistencyRepro(Output))
            {
                test.FaultyOverflowPagesHandling_CannotModifyReadOnlyPages(initialNumberOfDocs, numberOfModifications, seed);
            }
        }
    }
}
