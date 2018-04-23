using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_10732 : NoDisposalNeeded
    {
        [Fact]
        public void Can_check_memory_status()
        {
            SmapsReader smapsReader = PlatformDetails.RunningOnLinux ? new SmapsReader(new[] {new byte[SmapsReader.BufferSize], new byte[SmapsReader.BufferSize]}) : null;
            LowMemoryNotification.Instance.CheckMemoryStatus(smapsReader);
        }
    }
}
