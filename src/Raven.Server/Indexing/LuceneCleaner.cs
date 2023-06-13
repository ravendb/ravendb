using System;
using System.Threading;
using Lucene.Net.Search;
using Raven.Client.Util;
using Sparrow.LowMemory;

namespace Raven.Server.Indexing;

public class LuceneCleaner : ILowMemoryHandler
{
    private readonly ReaderWriterLockSlim _runningQueryLock = new();

    public LuceneCleaner()
    {
        LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        if (_runningQueryLock.TryEnterWriteLock(10) == false)
            return;

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
            _runningQueryLock.ExitWriteLock();
        }
    }

    public void LowMemoryOver()
    {
    }

    public IDisposable EnterRunningQueryReadLock()
    {
        _runningQueryLock.EnterReadLock();

        return new DisposableAction(() => _runningQueryLock.ExitReadLock());
    }
}
