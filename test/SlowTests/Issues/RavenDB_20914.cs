using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Http;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20914 : ClusterTestBase
{
    public RavenDB_20914(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task ReadBalanceBehavior_FastestNode_Should_Not_Leak_Tasks()
    {
        var (_, leader) = await CreateRaftCluster(3);

        using (var leaderStore = GetDocumentStore(new Options
        {
            ReplicationFactor = 3,
            ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = ReadBalanceBehavior.FastestNode,
            Server = leader
        }))
        {
            Task[] tasks = null;

            leaderStore.GetRequestExecutor().ForTestingPurposesOnly().ExecuteOnAllToFigureOutTheFastestOnTaskCompletion = _ => Thread.Sleep(1000);
            leaderStore.GetRequestExecutor().ForTestingPurposesOnly().ExecuteOnAllToFigureOutTheFastestOnBeforeWait = t => tasks = t;


            using (var session = leaderStore.OpenAsyncSession())
            {
                await session.Query<object>().ToListAsync();
            }

            Assert.NotNull(tasks);
            Assert.NotEmpty(tasks);

            foreach (var task in tasks)
            {
                if (task.IsCompleted || task.IsCanceled || task.IsFaulted)
                    continue;

                if (task == RequestExecutor.NeverEndingRequest)
                    continue;

                throw new InvalidOperationException("Task is not completed!");
            }
        }
    }
}
