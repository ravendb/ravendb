using System;

namespace Voron.Impl.Scratch
{
    public delegate void ScratchSpaceChangedDelegate(long scratchSpaceChangeInBytes);

    public class ScratchSpaceUsageMonitor : IScratchSpaceMonitor, IDisposable
    {
        private ScratchSpaceChangedDelegate _increase;
        private ScratchSpaceChangedDelegate _decrease;

        public long ScratchSpaceInBytes;

        public void AddMonitor(IScratchSpaceMonitor monitor)
        {
            _increase += monitor.Increase;
            _decrease += monitor.Decrease;
        }

        public void Increase(long allocatedScratchSpaceInBytes)
        {
            ScratchSpaceInBytes += allocatedScratchSpaceInBytes;

            _increase?.Invoke(allocatedScratchSpaceInBytes);
        }

        public void Decrease(long releasedScratchSpaceInBytes)
        {
            ScratchSpaceInBytes -= releasedScratchSpaceInBytes;

            _decrease?.Invoke(releasedScratchSpaceInBytes);
        }

        public void Dispose()
        {
            _increase = null;
            _decrease = null;
        }
    }
}
