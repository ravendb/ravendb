using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Cluster;

public class ClusterTransactionTestsStress : ClusterTestBase
{
    public ClusterTransactionTestsStress(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClusterTransactions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanCreateClusterTransactionRequest2(Options options)
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;
        var (_, leader) = await CreateRaftCluster(2);
        options.Server = leader;
        options.ReplicationFactor = 2;

        using (var leaderStore = GetDocumentStore(options))
        {
            var count = 0;
            var parallelism = RavenTestHelper.DefaultParallelOptions.MaxDegreeOfParallelism;

            for (var i = 0; i < 10; i++)
            {
                var tasks = new List<Task>();
                for (var j = 0; j < parallelism; j++)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                        {
                            TransactionMode = TransactionMode.ClusterWide
                        }))
                        {
                            session.Advanced.ClusterTransaction.CreateCompareExchangeValue($"usernames/{Interlocked.Increment(ref count)}", new User());
                            await session.SaveChangesAsync();
                        }

                        await ActionWithLeader((l) =>
                        {
                            l.ServerStore.Engine.CurrentLeader?.StepDown();
                            return Task.CompletedTask;
                        });
                    }));
                }

                await Task.WhenAll(tasks.ToArray());
                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var results = await session.Advanced.ClusterTransaction.GetCompareExchangeValuesAsync<User>(
                        Enumerable.Range(i * parallelism, parallelism).Select(x =>
                            $"usernames/{Interlocked.Increment(ref count)}").ToArray());
                    Assert.Equal(parallelism, results.Count);
                }
            }
        }
    }
}
