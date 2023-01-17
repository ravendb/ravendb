using Lucene.Net.Search;
using Sparrow.LowMemory;

namespace Raven.Server.Indexing;

public class LuceneCleaner : ILowMemoryHandler
{
    public LuceneCleaner()
    {
        LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        FieldCache_Fields.DEFAULT.PurgeAllCaches();
    }

    public void LowMemoryOver()
    {
    }
}
