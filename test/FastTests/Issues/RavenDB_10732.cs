using Sparrow.LowMemory;
using Sparrow.Server.LowMemory;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_10732 : NoDisposalNeeded
    {
        public RavenDB_10732(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_check_memory_status()
        {
            LowMemoryNotification.Instance.CheckMemoryStatus(LowMemoryMonitor.Instance);
        }
    }
}
