using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Server;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22031 : ClusterTestBase
{
    public RavenDB_22031(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Cluster)]
    public async Task RaftLogTrancateShouldWorkOnFollowers()
    {
        var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

        using var store = GetDocumentStore(new Options
        {
            Server = leader,
            ReplicationFactor = 3
        });

        for (int i = 0; i < 10; i++)
        {
            var user = new User
            {
                Id = $"Users/{i}-A",
                Name = $"User{i}"
            };

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }
        }

        long[] min = new long[3];
        long[] max = new long[3];

        var result = await WaitForValueAsync(() =>
        {
            for (int i = 0; i < 3; i++)
            {
                (min[i], max[i]) = GetLogEntriesRange(nodes[i]);
            }

            return min[0] == min[1] && min[1] == min[2] &&
                            max[0] == max[1] && max[1] == max[2];
        }, true);

        Assert.True(result, $"log history ranges of the nodes are different. leader is {leader.ServerStore.NodeTag}. ranges: {GetRangesString(min, max, nodes)}");
    }

    private string GetRangesString(long[] min, long[] max, List<RavenServer> nodes)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < 3; i++)
        {
            sb.Append(nodes[i].ServerStore.NodeTag).Append("-[").Append(min[i]).Append(", ").Append(max[i]).Append("] ");
        }

        return sb.ToString();
    }

    private (long Min, long Max) GetLogEntriesRange(RavenServer ravenServer)
    {
        using (ravenServer.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenWriteTransaction())
        {
            return ravenServer.ServerStore.Engine.GetLogEntriesRange(context);
        }
    }


    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
