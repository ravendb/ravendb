#nullable enable

using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    public class State: IDisposable
    {
        public readonly Pager Pager;

        public readonly WeakReference<State> WeakSelf;

        public State(Pager pager, byte* readAddress, byte* writeMemory, long totalAllocatedSize, void* handle)
        {
            ReadAddress = readAddress;
            WriteAddress = writeMemory;
            TotalAllocatedSize = totalAllocatedSize;
            NumberOfAllocatedPages = totalAllocatedSize / Constants.Storage.PageSize;
            Handle = handle;
       
            Pager = pager;
            WeakSelf = new WeakReference<State>(this);
        }


        public readonly byte* ReadAddress;
        public readonly byte* WriteAddress;
        public long NumberOfAllocatedPages;
        public long TotalAllocatedSize;
        public long TotalFileSize;
        public long TotalDiskSpace;

        public bool Disposed;

        public void* Handle;

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

        ~State()
        {
            try
            {
                Dispose();
            }
            catch (Exception e)
            {
                try
                {
                    // cannot throw an exception from here, just log it
                    var entry = new LogEntry
                    {
                        At = DateTime.UtcNow,
                        Logger = nameof(State),
                        Exception = e,
                        Message = "Failed to dispose the pager state from the finalizer",
                        Source = "PagerState Finalizer",
                        Type = LogMode.Operations
                    };
                    if (LoggingSource.Instance.IsOperationsEnabled)
                    {
                        LoggingSource.Instance.Log(ref entry);
                    }
                }
                catch
                {
                    // nothing we can do here
                }
            }
        }
    }
}
