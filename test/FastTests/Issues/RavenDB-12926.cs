using System;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Server;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_12926 : RavenTestBase
    {
        [Fact]
        public void AsyncManualResetEventWaitAsyncWithCancellationShouldWork()
        {
            var amre = new AsyncManualResetEvent();
            var cts = new CancellationTokenSource();
            var are = new AutoResetEvent(false);
            Task.Run(async () =>
            {
                try
                {
                    are.Set();
                    await amre.WaitAsync(cts.Token);
                    Assert.True(false, "AsyncManualResetEvent is expected to throw when canceled and it didn't");
                }
                finally
                {
                    are.Set();
                }
            });
            are.WaitOne();

            //I want to test the AsyncManualResetEvent when the Cts is not canceled so i'm waiting abit
            var start = DateTime.UtcNow;
            SpinWait.SpinUntil(() => DateTime.UtcNow - start > TimeSpan.FromMilliseconds(100));
            cts.Cancel();
            Assert.True(are.WaitOne(TimeSpan.FromSeconds(5)), "Waited for 30sec for AsyncManualResetEvent to be canceled be it didn't");
        }
    }
}
