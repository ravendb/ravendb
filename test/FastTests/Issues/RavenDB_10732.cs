using Sparrow.LowMemory;
using Sparrow.Server.LowMemory;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_10732 : NoDisposalNeeded
    {
        [Fact]
        public void Can_check_memory_status()
        {
            LowMemoryNotification.Instance.CheckMemoryStatus(LowMemoryMonitor.Instance);
        }
    }
}
