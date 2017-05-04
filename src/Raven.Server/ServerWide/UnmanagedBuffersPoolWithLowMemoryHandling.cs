using Sparrow.Json;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide
{
    public class UnmanagedBuffersPoolWithLowMemoryHandling : UnmanagedBuffersPool, ILowMemoryHandler
    {
        public UnmanagedBuffersPoolWithLowMemoryHandling(string debugTag, string databaseName = null) : base(debugTag, databaseName)
        {
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }


        public LowMemoryHandlerStatistics GetStats()
        {
            return new LowMemoryHandlerStatistics
            {
                Name = _debugTag,
                DatabaseName = _databaseName,
                EstimatedUsedMemory = GetAllocatedMemorySize()
            };
        }

    }
}