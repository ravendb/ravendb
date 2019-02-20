namespace Sparrow.LowMemory
{
    public interface ILowMemoryHandler
    {
        void LowMemory();
        void LowMemoryOver();
    }
}