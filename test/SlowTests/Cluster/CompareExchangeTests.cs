using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster;

public class CompareExchangeTests : RavenTestBase
{
    public CompareExchangeTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.CompareExchange)]
    public async Task AddOrUpdateCompareExchangeCommand_WhenCommandSentTwice_SecondAttemptShouldNotReturnNull()
    {
        var leader = GetNewServer();
        using var store = GetDocumentStore(new Options { Server = leader});

        var longCommandTasks =  Enumerable.Range(0, 5 * 1024).Select(i => Task.Run(async () =>
        {
            string uniqueRequestId = RaftIdGenerator.NewId();
            string mykey = $"mykey{i}";
            var timeoutAttemptTask = Task.Run(async () =>
            {
                try
                {
                    using JsonOperationContext context = JsonOperationContext.ShortTermSingleUse();
                    var value = context.ReadObject(new DynamicJsonValue {[$"prop{i}"] = "my value"}, "compare exchange");
                    var toRunTwiceCommand1 = new AddOrUpdateCompareExchangeCommand(store.Database, mykey, value, 0, context, uniqueRequestId);
                    toRunTwiceCommand1.Timeout = TimeSpan.FromSeconds(1);
                    await leader.ServerStore.Engine.CurrentLeader.PutAsync(toRunTwiceCommand1, toRunTwiceCommand1.Timeout.Value);
                }
                catch (TimeoutException)
                {
                    // ignored
                }
            });

            await Task.Delay(1);
                
            using JsonOperationContext context = JsonOperationContext.ShortTermSingleUse();
            var value = context.ReadObject(new DynamicJsonValue {[$"prop{i}"] = "my value"}, "compare exchange");
            var toRunTwiceCommand2 = new AddOrUpdateCompareExchangeCommand(store.Database, mykey, value, 0, context, uniqueRequestId);
            toRunTwiceCommand2.Timeout = TimeSpan.FromSeconds(200);
            var (_, result) = await leader.ServerStore.Engine.CurrentLeader.PutAsync(toRunTwiceCommand2, toRunTwiceCommand2.Timeout.Value);
            Assert.NotNull(result);
            var compareExchangeResult = (CompareExchangeCommandBase.CompareExchangeResult)result;
            Assert.Equal(context, ((BlittableJsonReaderObject)compareExchangeResult.Value)._context);
            Assert.Equal(value, compareExchangeResult.Value);

            await timeoutAttemptTask;
        })).ToArray();

        await Task.WhenAll(longCommandTasks);
    }
    
    [RavenTheory(RavenTestCategory.ClusterTransactions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task DeletingCompareExchangeCommand_WhenNoModificationsOnTheDatabase_ShouldDeleteTombstone(Options options)
    {
        var settings = new Dictionary<string, string>
        {
            { RavenConfiguration.GetKey(x => x.Cluster.MaxClusterTransactionCompareExchangeTombstoneCheckInterval), "0" },
            { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "0" },
        };
        var server = GetNewServer(new ServerCreationOptions{CustomSettings = settings});
            
        options.Server = server;
        using (var store = GetDocumentStore(options))
        {
            var saveResult = store.Operations.Send(new PutCompareExchangeValueOperation<string>("key", "value", 0));
            store.Operations.Send(new DeleteCompareExchangeValueOperation<string>("key", saveResult.Index));

            await WaitForValueAsync(async () =>
            {
                var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                return stats.CountOfCompareExchangeTombstones;
            }, 0, timeout: 15 * 1000);
        }
    }
}
