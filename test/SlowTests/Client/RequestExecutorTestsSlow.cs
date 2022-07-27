using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class RequestExecutorTestsSlow : RavenTestBase
    {
        public RequestExecutorTestsSlow(ITestOutputHelper output) : base(output)
        {
        }

        [LicenseRequiredTheory]
        [InlineData(1, 2, "OnBeforeRequest", "OnFailedRequest", "OnBeforeRequest", "OnAfterRequests")]
        [InlineData(2, 2, "OnBeforeRequest", "OnFailedRequest", "OnBeforeRequest")]
        public async Task OnBeforeAfterAndFailRequest(int failCount, int clusterSize, params string[] expected)
        {
            using (var test = new RequestExecutorTests(Output))
            {
                await test.OnBeforeAfterAndFailRequest(failCount, clusterSize, expected);
            }
        }
    }
}
