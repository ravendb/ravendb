using System.Threading;
using Sparrow;
using Voron.Impl.Scratch;

namespace Raven.Server.Documents.Indexes
{
    public class GlobalIndexingScratchSpaceMonitor : IScratchSpaceMonitor
    {
        public GlobalIndexingScratchSpaceMonitor(Size indexingGlobalScratchSpaceLimit)
        {
            LimitInBytes = indexingGlobalScratchSpaceLimit.GetValue(SizeUnit.Bytes);
        }

        public readonly long LimitInBytes;

        public Size LimitAsSize => new Size(LimitInBytes, SizeUnit.Bytes);

        public long ScratchSpaceInBytes;

        public Size ScratchSpaceAsSize => new Size(ScratchSpaceInBytes, SizeUnit.Bytes);

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
