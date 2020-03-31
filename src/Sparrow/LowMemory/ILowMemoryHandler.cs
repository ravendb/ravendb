namespace Sparrow.LowMemory
{
    public interface ILowMemoryHandler
    {
        void LowMemory(bool extremelyLow);
        void LowMemoryOver();
    }
}
