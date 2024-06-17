#nullable enable

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Reflection.Metadata;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Voron.Platform.Win32;

namespace Voron.Impl.Paging;

public unsafe partial class Pager2
{
    public class TxStateFor32Bits
    {
        public Dictionary<long, LoadedPage> LoadedPages = [];
        public List<MappedAddresses> AddressesToUnload = [];
        public long TotalLoadedSize;
    }

    public struct PagerTransactionState 
    {
        public Dictionary<Pager2, TxStateFor32Bits> For32Bits;
        public Dictionary<Pager2, CryptoTransactionState> ForCrypto;
        public bool IsWriteTransaction;
        
        /// <summary>
        /// These are events because we may have a single transaction deal
        /// with multiple pagers 
        /// </summary>
        public event TxStateDelegate OnDispose;
        public event TxStateDelegate OnBeforeCommitFinalization;
        
        /// <summary>
        /// This is an action because in sync, we have a *single* pager invovled
        /// </summary>
        public TxStateDelegate Sync;

        public delegate void TxStateDelegate(Pager2 pager, State state, ref PagerTransactionState txState);

        public void InvokeBeforeCommitFinalization(Pager2 pager, State state, ref PagerTransactionState txState) => OnBeforeCommitFinalization?.Invoke(pager, state, ref txState);
        public void InvokeDispose(Pager2 pager, State state, ref PagerTransactionState txState) => OnDispose?.Invoke(pager, state, ref txState);
    }
    
    public class State: IDisposable
    {
        string S = Environment.StackTrace;
        public readonly Pager2 Pager;
        
        /// <summary>
        /// For the duration of the transaction that created this state, we *must*
        /// hold a hard reference to the previous state(s) to ensure that any pointer
        /// that we _already_ got is valid.
        ///
        /// This is cleared upon committing the transaction state to the global state  
        /// </summary>
        private State? _previous;

        public void BeforePublishing()
        {
            _previous = null;
        }

        public readonly WeakReference<State> WeakSelf;
        public State(Pager2 pager, State? previous)
        {
            Pager = pager;
            _previous = previous;
            WeakSelf = new WeakReference<State>(this);
        }

        public State Clone()
        {
            State clone = new State(Pager, this)
            {
                FileAccess = FileAccess,
                FileStream = FileStream,
                Handle = Handle,
                MemAccess = MemAccess,
                FileAttributes = FileAttributes,
            };
            return clone;
        }

        public bool _fileOwnershipMoved;
        

        public byte* BaseAddress;
        public long NumberOfAllocatedPages;
        public long TotalAllocatedSize;
        public MemoryMappedFile? MemoryMappedFile;

        public bool Disposed;

        public Win32NativeFileAccess FileAccess;
        public Win32NativeFileAttributes FileAttributes;
        public Win32MemoryMapNativeMethods.NativeFileMapAccessType MemAccess;
        public SafeFileHandle Handle;
        public FileStream FileStream;

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

                if (_fileOwnershipMoved == false)
                {
                    Handle.Dispose();
                    FileStream.Dispose();
                }

                MemoryMappedFile?.Dispose();
                if (PlatformDetails.RunningOnWindows)
                {
                    Win32MemoryMapNativeMethods.UnmapViewOfFile(BaseAddress);
                }
                else
                {
                    throw new NotImplementedException();
                }
                NativeMemory.UnregisterFileMapping(Pager.FileName, (nint)BaseAddress, TotalAllocatedSize);
            }
            
            GC.SuppressFinalize(this);

        }

        ~State()
        {
            // this is here only to avoid the "field is unused" error
            GC.KeepAlive(_previous);
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

        public void MoveFileOwnership()
        {
            _fileOwnershipMoved = true;
        }
    }

}
