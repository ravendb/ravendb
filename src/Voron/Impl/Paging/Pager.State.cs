#nullable enable

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Voron.Global;
using Voron.Logging;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    public class State : IDisposable
    {
        public readonly Pager Pager;

        public readonly WeakReference<State> WeakSelf;

        public State(Pager pager, byte* readAddress, byte* writeMemory, long totalAllocatedSize, void* handle, int pageSize)
        {
            ReadAddress = readAddress;
            WriteAddress = writeMemory;
            TotalAllocatedSize = totalAllocatedSize;
            Handle = handle;
            _pageSize = pageSize;

            Pager = pager;
            WeakSelf = new WeakReference<State>(this);
            NativeMemory.RegisterFileMapping(pager.FileName, new IntPtr(ReadAddress), TotalAllocatedSize, null);
        }


        public readonly byte* ReadAddress;
        public readonly byte* WriteAddress;
        public long NumberOfAllocatedPages => TotalAllocatedSize / _pageSize;
        public long TotalAllocatedSize;
        public long TotalPhysicalSpace;

        public bool Disposed;

        public void* Handle;
        private readonly int _pageSize;

        public void Dispose()
        {
            if (Disposed)
                return;
            // we may call this via a weak reference, so we need to ensure that 
            // we aren't racing through the finalizer and explicit dispose
            lock (WeakSelf)
            {
                if (Disposed)
                    return;

                Disposed = true;

                Pager._states.TryRemove(WeakSelf);

                var rc = Pal.rvn_close_pager(Handle, out var errorCode);
                NativeMemory.UnregisterFileMapping(Pager.FileName, (nint)ReadAddress, TotalAllocatedSize);

                if (rc != PalFlags.FailCodes.Success)
                {
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to close data pager for: {Pager.FileName}");
                }
            }

            GC.SuppressFinalize(this);
        }

        // Closing a pager is munmap + close. OS takes _global page table lock_ each time. munmap also does TLB flushes, which are expensive. 
        // Under load, we may grow a file by multiple increments quickly, so when the finalizer runs, we'll have multiple page instances to dispose.
        // To avoid hammering the system, we run through this one at a time. Anyone waiting on pager disposal should be calling it explicitly anyway.
        private static readonly ConcurrentQueue<State> PendingDisposal = new();
        private static int _disposalScheduled;

        private static void ScheduleBackgroundDisposal()
        {
            if (Interlocked.CompareExchange(ref _disposalScheduled, 1, 0) != 0)
                return;

            ThreadPool.UnsafeQueueUserWorkItem(static _ =>
            {
                do
                {
                    while (PendingDisposal.TryDequeue(out var state))
                    {
                        try
                        {
                            state.Dispose();
                        }
                        catch (Exception e)
                        {
                            try
                            {
                                // cannot let the drain die, just log it
                                var logger = RavenLogManager.Instance.GetLoggerForGlobalVoron<State>();

                                if (logger.IsErrorEnabled)
                                {
                                    logger.Error("Failed to dispose a pager state from the background disposer", e);
                                }
                            }
                            catch
                            {
                                // nothing we can do here
                            }
                        }
                    }

                    Volatile.Write(ref _disposalScheduled, 0);
                    // a producer may have enqueued between the last TryDequeue and the gate
                    // release; re-arm and keep draining if we win the gate back
                } while (PendingDisposal.IsEmpty == false &&
                         Interlocked.CompareExchange(ref _disposalScheduled, 1, 0) == 0);
            }, null);
        }

        ~State()
        {
            try
            {
                // resurrecting the instance is fine: the queue holds the only reference
                // until Dispose completes, and Dispose is idempotent under its lock
                PendingDisposal.Enqueue(this);
                ScheduleBackgroundDisposal();
            }
            catch
            {
                // queueing failed (shutdown, OOM) - the native handle leaks rather than
                // risking a blocking syscall storm on the finalizer thread
            }
        }
    }
}
