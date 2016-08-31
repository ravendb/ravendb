using Raven.Client.Util;
using Raven.Server.ServerWide.LowMemoryNotification;

namespace Raven.Server.ServerWide
{
    public class UnmanagedBuffersPoolWithLowMemoryHandling : UnmanagedBuffersPool, ILowMemoryHandler
    {
        public UnmanagedBuffersPoolWithLowMemoryHandling(string debugTag, string databaseName = null) : base(debugTag, databaseName)
        {
            AbstractLowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
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