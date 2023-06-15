using System;
using System.Threading;
using Lucene.Net.Search;
using Nito.AsyncEx;
using Sparrow.LowMemory;

namespace Raven.Server.Indexing;

public class LuceneCleaner : ILowMemoryHandler
{
    private readonly AsyncReaderWriterLock _runningQueryLock = new();

    public LuceneCleaner()
    {
        LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        IDisposable writeLock;

        using (var cts = new CancellationTokenSource())
        {
            cts.CancelAfter(10);
            try
            {
                writeLock = _runningQueryLock.WriterLock(cts.Token);
            }
            catch
            {
                return;
            }
        }

        try
        {
            // PurgeAllCaches is replacing the cache with a new one (without actually releasing any memory).
            // When the GC will run, the finalizer of the Segments will be executed and release the unmanaged memory.
            // HOWEVER, this will happen when the managed memory is high enough to trigger a GC cycle.
            // Which is too late since we are already in a low memory state
            FieldCache_Fields.DEFAULT.PurgeAllCaches();
        }
        finally
        {
            writeLock.Dispose();
        }
    }

    public void LowMemoryOver()
    {
    }

    public IDisposable EnterRunningQueryReadLock()
    {
        return _runningQueryLock.ReaderLock();
    }
}
