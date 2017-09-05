using System.Threading.Tasks;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{
    public class BasicTests : RachisConsensusTestBase
    {
        [NightlyBuildTheory]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task CanApplyCommitAcrossAllCluster(int amount)
        {
            var leader = await CreateNetworkAndGetLeader(amount);
            long lastIndex = 0;
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                for (var i = 0; i < 5; i++)
                {
                    var (index, _) = await leader.PutAsync(new TestCommand { Name = "test", Value = i });
                    lastIndex = index;
                }
            }

            foreach (var r in RachisConsensuses)
            {
                var condition = await
                    r.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex)
                        .WaitAsync(5000);
                TransactionOperationContext context;
                using (r.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    var commitIndex = r.GetLastCommitIndex(context);
                    Assert.True(condition, $"Last commit is {commitIndex} wanted {lastIndex}");
                }
            }

        }
    }
}
