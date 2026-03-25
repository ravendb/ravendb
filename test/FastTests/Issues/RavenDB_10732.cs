using Sparrow.LowMemory;
using Sparrow.Server.LowMemory;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_10732 : NoDisposalNeeded
    {
        public RavenDB_10732(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Can_check_memory_status()
        {
            using (var monitor = new LowMemoryMonitor())
            {
                var lowMemoryNotification = new LowMemoryNotification();
                lowMemoryNotification.CheckMemoryStatus(monitor);
            }
        }
    }
}
