using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using SlowTests.Server.Replication;
using Tests.Infrastructure.InterversionTest;
using Xunit;

namespace StressTests.Server.Replication
{
    public class ExternalReplicationStressTests : ReplicationTestBase
    {
        [Fact]
        public async Task ExternalReplicationShouldWorkWithSmallTimeoutStress()
        {
            var cts = new CancellationTokenSource();
            Exception exception = null;
            Task<string> loggingTask = null;
            var adminStore = GetDocumentStore();
            try
            {
                
#pragma warning disable 4014
                loggingTask = await Task.Run(async () =>
                    WebSocketUtil.CollectAdminLogs(adminStore, cts.Token));
#pragma warning restore 4014            


                for (int i = 0; i < 100; i++)
                {
                    Parallel.For(0, 10, RavenTestHelper.DefaultParallelOptions, _ =>
                    {
                        using (var test = new ExternalReplicationTests())
                        {
                            test.ExternalReplicationShouldWorkWithSmallTimeoutStress().Wait();
                        }
                    });
                }
            }
            catch (Exception e)
            {
                exception = e;
            }
            finally
            {
                cts.Cancel();
                adminStore.Dispose();
                if (loggingTask != null)
                {
                    var res = await loggingTask;
                    if (exception != null)
                    {
                        throw new InvalidOperationException($"ExternalReplicationShouldWorkWithSmallTimeoutStress failed. Full log is: \r\n{res}", exception);
                    }
                    File.Delete(res);
                }                
            }
        }
    }
}
