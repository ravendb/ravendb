using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class CriteriaScript : SubscriptionTestBase
    {
        [Fact]
        public async Task BasicCriteriaTest_WithSsl()
        {
            using (var x = new FastTests.Client.Subscriptions.CriteriaScript())
            {
                await x.BasicCriteriaTest(useSsl: true);
            }
        }

        [Fact]
        public async Task CriteriaScriptWithTransformation_WithSsl()
        {
            using (var x = new FastTests.Client.Subscriptions.CriteriaScript())
            {
                await x.CriteriaScriptWithTransformation(useSsl: true);
            }
        }
    }
}
