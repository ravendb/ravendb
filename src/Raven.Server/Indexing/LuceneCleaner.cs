using System;
using System.Diagnostics;
using System.Threading;
using Lucene.Net.Search;
using Nito.AsyncEx;
using Sparrow;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Raven.Server.Indexing;

public class LuceneCleaner : ILowMemoryHandler
{
    private readonly AsyncReaderWriterLock _runningQueryLock = new();
    private long _lowMemoryOverGeneration;
    private long _lowMemoryGeneration = -1;
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LuceneCleaner>("Memory");

    public LuceneCleaner()
    {
        LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        if (_lowMemoryGeneration == _lowMemoryOverGeneration)
        {
            // we've already run cleanup for this low memory cycle
            return;
        }

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

        IDisposable cacheToDispose;
        long unmanagedUsedBeforeInBytes = NativeMemory.TotalLuceneUnmanagedAllocationsForSorting;

        try
        {
            // PurgeAllCaches is replacing the cache with a new one (without actually releasing any memory).
            // When the GC will run, the finalizer of the Segments will be executed and release the unmanaged memory.
            // HOWEVER, this will happen when the managed memory is high enough to trigger a GC cycle.
            // Which is too late since we are already in a low memory state
            cacheToDispose = FieldCache_Fields.DEFAULT.PurgeAllCaches();
        }
        finally
        {
            writeLock.Dispose();
        }

        _lowMemoryGeneration = _lowMemoryOverGeneration;

        Stopwatch sp = Logger.IsInfoEnabled ? Stopwatch.StartNew() : null;

        cacheToDispose.Dispose();

        if (sp != null && sp.ElapsedMilliseconds > 100)
        {
            Logger.Info($"Purged Lucene caches, took: {sp.ElapsedMilliseconds}ms, " +
                        $"cleaned: {new Size(unmanagedUsedBeforeInBytes - NativeMemory.TotalLuceneUnmanagedAllocationsForSorting, SizeUnit.Bytes)}");
        }
    }

    public void LowMemoryOver()
    {
        _lowMemoryOverGeneration++;
    }

    public IDisposable EnterRunningQueryReadLock()
    {
        return _runningQueryLock.ReaderLock();
    }
}
