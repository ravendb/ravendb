namespace Sparrow.LowMemory
{
    public interface ILowMemoryHandler
    {
        void LowMemory(LowMemSeverity lowMemSeverity);
        void LowMemoryOver();
    }
}
