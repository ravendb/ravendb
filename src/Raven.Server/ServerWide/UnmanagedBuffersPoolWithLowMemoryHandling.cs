using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Server.Logging;

namespace Raven.Server.ServerWide
{
    public sealed class UnmanagedBuffersPoolWithLowMemoryHandling : UnmanagedBuffersPool, ILowMemoryHandler
    {
        public UnmanagedBuffersPoolWithLowMemoryHandling(RavenLogger logger, string debugTag, string databaseName = null) : base(logger, debugTag, databaseName)
        {
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }
    }
}
