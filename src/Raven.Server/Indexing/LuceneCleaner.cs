using System;
using System.Diagnostics;
using System.Threading;
using Lucene.Net.Search;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Raven.Server.Indexing;

public class LuceneCleaner : ILowMemoryHandler
{
    private readonly ReaderWriterLockSlim _runningQueryLock = new();
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LuceneCleaner>("Memory");

    public LuceneCleaner()
    {
        LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        if (_runningQueryLock.TryEnterWriteLock(10) == false)
            return;

        IDisposable toDispose;
        long unmanagedUsedBeforeInBytes = NativeMemory.TotalAllocatedMemoryByLucene;

        try
        {
            // PurgeAllCaches is replacing the cache with a new one (without actually releasing any memory).
            // When the GC will run, the finalizer of the Segments will be executed and release the unmanaged memory.
            // HOWEVER, this will happen when the managed memory is high enough to trigger a GC cycle.
            // Which is too late since we are already in a low memory state
            toDispose = FieldCache_Fields.DEFAULT.PurgeAllCaches();
        }
        finally
        {
            _runningQueryLock.ExitWriteLock();
        }

        Stopwatch sp = Logger.IsInfoEnabled ? Stopwatch.StartNew() : null;

        toDispose.Dispose();

        if (sp != null && sp.ElapsedMilliseconds > 100)
        {
            Logger.Info($"Purged Lucene caches, took: {sp.ElapsedMilliseconds}ms, " +
                        $"cleaned: {new Sparrow.Size(NativeMemory.TotalAllocatedMemoryByLucene - unmanagedUsedBeforeInBytes, SizeUnit.Bytes)}");
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
