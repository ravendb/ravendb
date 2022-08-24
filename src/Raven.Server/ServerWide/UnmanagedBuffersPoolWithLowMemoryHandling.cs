using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide
{
    public class UnmanagedBuffersPoolWithLowMemoryHandling : UnmanagedBuffersPool, ILowMemoryHandler
    {
        public UnmanagedBuffersPoolWithLowMemoryHandling(string debugTag, Logger logger = null) : base(debugTag, logger)
        {
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }
    }
}
