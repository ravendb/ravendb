using System.Threading.Tasks;
using SlowTests.Server.Documents.Expiration;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Core.Expiration
{
    public class ExpirationStressTest : NoDisposalNoOutputNeeded
    {
        public ExpirationStressTest(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(1000)]
        [InlineData(10000)]
        public async Task CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(int count)
        {
            await using (var expiration = new ExpirationTests(Output))
            {
                await expiration.CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(false, count);
            }
        }
    }
}