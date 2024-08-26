using System;
using System.Threading;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_12623 : NoDisposalNeeded
    {
        public RavenDB_12623(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ContextPoolsShouldNotLeakThreadIdData()
        {            
            var p1 = new TransactionContextPool(RavenLogManager.CreateNullLogger(), new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()));
            var p2 = new TransactionContextPool(RavenLogManager.CreateNullLogger(), new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()));

            p1.AllocateOperationContext(out JsonOperationContext ctx);

            var t = new Thread(() =>
            {
                p2.AllocateOperationContext(out JsonOperationContext _1);
                p1.AllocateOperationContext(out JsonOperationContext _).Dispose();
            });
            t.Start();
            p2.AllocateOperationContext(out JsonOperationContext ___);

            t.Join();

            t = null;

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            t = new Thread(() =>
            {

                p1.AllocateOperationContext(out JsonOperationContext _);
                p1 = null;
                p2.AllocateOperationContext(out JsonOperationContext ctx2);

                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                p2.AllocateOperationContext(out JsonOperationContext ctx3);
            });
            t.Start();
            t.Join();
        }
    }
}
