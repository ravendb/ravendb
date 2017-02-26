using System;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Rachis
{
    public class BasicTests : RachisConsensusTestBase
    {
        [Theory]
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
                    lastIndex =  await leader.PutAsync(ctx.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "test",
                        ["Value"] = i 
                    }, "test"));
                }
            }

            foreach (var r in RachisConsensuses)
            {
                Assert.True(r.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex).Wait(5000));   
            }
            
        }
    }
}
