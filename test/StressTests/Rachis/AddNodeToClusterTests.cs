using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Rachis
{
    public class AddNodeToClusterTests : NoDisposalNoOutputNeeded
    {
        public AddNodeToClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        [NightlyBuildMultiplatformTheory(RavenArchitecture.AllX64)]
        [InlineData(true)]
        public async Task ReAddMemberNode(bool withManyCompareExchange)
        {
            using (var store = new RachisTests.AddNodeToClusterTests(Output))
            {
                await store.ReAddMemberNode(withManyCompareExchange);
            }
        }
    }
}
