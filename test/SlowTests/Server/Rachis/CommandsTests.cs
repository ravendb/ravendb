using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Rachis
{
    public class CommandsTests: RachisConsensusTestBase
    {
        [Fact]
        public async Task When_command_committed_CompletionTaskSource_is_notified()
        {
            const int CommandCount = 10;
            const int clusterSize = 3;
            var leader = await CreateNetworkAndGetLeader(clusterSize);
            var nonLeader = GetFirstNonLeader();
            var tasks = new List<Task>();
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                for (var i = 0; i < CommandCount; i++)
                {
                    tasks.Add(leader.PutAsync(ctx.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "test",
                        ["Value"] = i
                    }, "test")));
                }
            }
            bool indexChanged = true;
            while (indexChanged)
            {
                indexChanged = nonLeader.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.AnyChange, 0).Wait(1000);
            }
            Assert.True(tasks.All(t=>t.Status == TaskStatus.RanToCompletion),"Some commands didn't complete");
        }
    }
}
