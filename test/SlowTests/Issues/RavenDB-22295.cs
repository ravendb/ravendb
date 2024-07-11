using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues;
internal class RavenDB_22295 : ClusterTestBase
{
    public RavenDB_22295(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Cluster)]
    public async Task UnobservedObjectDisposedExceptionAfterLeaderDispose()
    {
        Exception e = null;
        EventHandler<UnobservedTaskExceptionEventArgs> handler = (sender, args) =>
        {
            if(e is ObjectDisposedException)
                e = args.Exception;
        };

        TaskScheduler.UnobservedTaskException += handler;

        var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: false, shouldRunInMemory: false);
        leader.Dispose();

        GC.Collect();
        GC.WaitForPendingFinalizers();

        try
        {
            if (e != null)
                throw e;
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= handler;
        }
    }
}

