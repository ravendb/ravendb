using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
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
        var databaseName = GetDatabaseName();
        var (_, leader) = await CreateRaftCluster(3);

        var (index, _) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);
        await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(30));

        using (var leaderStore = new DocumentStore
        {
            Urls = new[] { leader.WebUrl },
            Database = databaseName,
            Conventions =
            {
                ReadBalanceBehavior = ReadBalanceBehavior.FastestNode
            }
        })
        {
            leaderStore.Initialize();

            Task[] tasks = null;

            leaderStore.GetRequestExecutor().ForTestingPurposesOnly().ExecuteOnAllToFigureOutTheFastestOnTaskCompletion = _ => Thread.Sleep(1000);
            leaderStore.GetRequestExecutor().ForTestingPurposesOnly().ExecuteOnAllToFigureOutTheFastestOnBeforeWait = t => tasks = t;


            using (var session = leaderStore.OpenAsyncSession())
            {
                await session.Query<object>().ToListAsync();
            }

            Assert.NotNull(tasks);

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
