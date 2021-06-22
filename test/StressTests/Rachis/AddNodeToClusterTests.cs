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

        [NightlyBuildTheory64Bit]
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
