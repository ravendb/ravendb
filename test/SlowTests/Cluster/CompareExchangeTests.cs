using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster;

public class CompareExchangeTests : RavenTestBase
{
    public CompareExchangeTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public async Task AddOrUpdateCompareExchangeCommand_WhenCommandSentTwice_SecondAttemptShouldNotReturnNull()
    {
        var leader = GetNewServer();
        using var store = GetDocumentStore(new Options { Server = leader});

        var longCommandTasks =  Enumerable.Range(0, 5 * 1024).Select(i => Task.Run(async () =>
        {
            string uniqueRequestId = RaftIdGenerator.NewId();
            string mykey = $"mykey{i}";
            _ = Task.Run(async () =>
            {
                using JsonOperationContext context = JsonOperationContext.ShortTermSingleUse();
                var value = context.ReadObject(new DynamicJsonValue {[$"prop{i}"] = "my value"}, "compare exchange");
                var toRunTwiceCommand1 = new AddOrUpdateCompareExchangeCommand(store.Database, mykey, value, 0, context, uniqueRequestId);
                toRunTwiceCommand1.Timeout = TimeSpan.FromSeconds(1);
                await leader.ServerStore.Engine.CurrentLeader.PutAsync(toRunTwiceCommand1, toRunTwiceCommand1.Timeout.Value);
            });

            await Task.Delay(1);
                
            using JsonOperationContext context = JsonOperationContext.ShortTermSingleUse();
            var value = context.ReadObject(new DynamicJsonValue {[$"prop{i}"] = "my value"}, "compare exchange");
            var toRunTwiceCommand2 = new AddOrUpdateCompareExchangeCommand(store.Database, mykey, value, 0, context, uniqueRequestId);
            toRunTwiceCommand2.Timeout = TimeSpan.FromSeconds(40);
            var (_, result) = await leader.ServerStore.Engine.CurrentLeader.PutAsync(toRunTwiceCommand2, toRunTwiceCommand2.Timeout.Value);
            Assert.NotNull(result);
            var compareExchangeResult = (CompareExchangeCommandBase.CompareExchangeResult)result;
            Assert.Equal(value, compareExchangeResult.Value);
        })).ToArray();

        await Task.WhenAll(longCommandTasks);
    }
}
