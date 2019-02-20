using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Sparrow.Server.LowMemory
{
    public class LowMemoryMonitor : AbstractLowMemoryMonitor
    {
        public static readonly LowMemoryMonitor Instance = new LowMemoryMonitor();

        private readonly SmapsReader _smapsReader;

        private LowMemoryMonitor()
        {
            _smapsReader = PlatformDetails.RunningOnLinux ? new SmapsReader(new[] {new byte[SmapsReader.BufferSize], new byte[SmapsReader.BufferSize]}) : null;
        }

        public override MemoryInfoResult GetMemoryInfoOnce()
        {
            return MemoryInformation.GetMemoryInformationUsingOneTimeSmapsReader();
        }

        public override MemoryInfoResult GetMemoryInfo(bool extended = false)
        {
            return MemoryInformation.GetMemoryInfo(_smapsReader, extended: extended);
        }

        public override bool IsEarlyOutOfMemory(MemoryInfoResult memInfo, out Size commitChargeThreshold)
        {
            return MemoryInformation.IsEarlyOutOfMemory(memInfo, out commitChargeThreshold);
        }

        public override void AssertNotAboutToRunOutOfMemory()
        {
            MemoryInformation.AssertNotAboutToRunOutOfMemory();
        }
    }
}
