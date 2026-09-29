using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_26999 : NoDisposalNoOutputNeeded
    {
        public RavenDB_26999(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [InlineData(1_000_000, 10)]
        public async Task ScoreIsComputedRegardlessOfPostingListSize(int totalDocuments, int rareEvery)
        {
            await using var testClass = new SlowTests.Issues.RavenDB_26999(Output);
            testClass.ScoreIsComputedRegardlessOfPostingListSize(totalDocuments, rareEvery);
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        public async Task CoraxScoresMatchLuceneOrderOfMagnitude()
        {
            await using var testClass = new SlowTests.Issues.RavenDB_26999(Output);
            testClass.CoraxScoresMatchLuceneOrderOfMagnitude();
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        public async Task ScoresStayOrderedByRelevanceAcrossPostingListSizes()
        {
            await using var testClass = new SlowTests.Issues.RavenDB_26999(Output);
            testClass.ScoresStayOrderedByRelevanceAcrossPostingListSizes();
        }
    }
}
