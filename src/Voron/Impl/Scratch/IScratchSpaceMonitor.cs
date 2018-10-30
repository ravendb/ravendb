namespace Voron.Impl.Scratch
{
    public interface IScratchSpaceMonitor
    {
        void Increase(long allocatedScratchSpaceInBytes);

        void Decrease(long releasedScratchSpaceInBytes);
    }
}
