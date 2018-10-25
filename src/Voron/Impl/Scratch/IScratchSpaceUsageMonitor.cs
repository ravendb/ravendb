namespace Voron.Impl.Scratch
{
    public interface IScratchSpaceUsageMonitor
    {
        void Increase(long allocatedScratchSpaceInBytes);

        void Decrease(long releasedScratchSpaceInBytes);
    }
}
