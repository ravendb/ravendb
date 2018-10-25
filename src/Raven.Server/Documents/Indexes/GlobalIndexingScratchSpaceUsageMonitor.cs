using System.Threading;
using Sparrow;
using Voron.Impl.Scratch;

namespace Raven.Server.Documents.Indexes
{
    public class GlobalIndexingScratchSpaceUsageMonitor : IScratchSpaceUsageMonitor
    {
        public GlobalIndexingScratchSpaceUsageMonitor(Size indexingGlobalScratchSpaceLimit)
        {
            LimitInBytes = indexingGlobalScratchSpaceLimit.GetValue(SizeUnit.Bytes);
        }

        public readonly long LimitInBytes;

        public long ScratchSpaceInBytes;

        public bool IsLimitExceeded => ScratchSpaceInBytes > LimitInBytes;

        public void Increase(long allocatedScratchSpaceInBytes)
        {
            Interlocked.Add(ref ScratchSpaceInBytes, allocatedScratchSpaceInBytes);
        }

        public void Decrease(long releasedScratchSpaceInBytes)
        {
            Interlocked.Add(ref ScratchSpaceInBytes, -releasedScratchSpaceInBytes);
        }
    }
}
