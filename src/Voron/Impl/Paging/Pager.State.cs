#nullable enable

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Reflection.Metadata;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Sparrow;
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

    
    public sealed class CryptoTransactionState : IEnumerable<KeyValuePair<long, EncryptionBuffer>> 
    {
        private Dictionary<long, EncryptionBuffer> _loadedBuffers = new Dictionary<long, EncryptionBuffer>();
        private long _totalCryptoBufferSize;

        /// <summary>
        /// Used for computing the total memory used by the transaction crypto buffers
        /// </summary>
        public long TotalCryptoBufferSize => _totalCryptoBufferSize;

        public void SetBuffers(Dictionary<long, EncryptionBuffer> loadedBuffers)
        {
            var total = 0L;
            foreach (var buffer in loadedBuffers.Values)
            {
                total += buffer.Size;
            }

            _loadedBuffers = loadedBuffers;
            _totalCryptoBufferSize = total;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(long pageNumber, out EncryptionBuffer value)
        {
            return _loadedBuffers.TryGetValue(pageNumber, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool RemoveBuffer(long pageNumber)
        {
            return _loadedBuffers.Remove(pageNumber);
        }

        public EncryptionBuffer this[long index]
        {
            get => _loadedBuffers[index];
            //This assumes that we don't replace buffers just set them.
            set
            {
                _loadedBuffers[index] = value;
                _totalCryptoBufferSize += value.Size;
            }
        }

        
        public IEnumerator<KeyValuePair<long, EncryptionBuffer>> GetEnumerator()
        {
            return _loadedBuffers.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
    
    

    public sealed class MappedAddresses
    {
        public string File;
        public IntPtr Address;
        public long StartPage;
        public long Size;
        public int Usages;
    }

    public sealed unsafe class LoadedPage
    {
        public byte* Pointer;
        public int NumberOfPages;
        public long StartPage;
    }
    
    public sealed unsafe class EncryptionBuffer(EncryptionBuffersPool pool)
    {
        public static readonly UIntPtr HashSize = Sodium.crypto_generichash_bytes();
        public static readonly int HashSizeInt = (int)Sodium.crypto_generichash_bytes();
        
        public readonly long Generation = pool.Generation;
        public long Size;
        public long OriginalSize;
        public byte* Pointer;

        public NativeMemory.ThreadStats AllocatingThread;
        public bool SkipOnTxCommit;
        public bool ExcessStorageFromAllocationBreakup;
        public bool Modified;
        public bool PartOfLargerAllocation;
        public int Usages;

        public bool CanRelease => Usages == 0;

        public void AddRef()
        {
            Usages++;
        }

        public void ReleaseRef()
        {
            Usages--;
        }
    }

    public struct PagerTransactionState 
    {
        public Dictionary<Pager2, TxStateFor32Bits>? For32Bits;
        public Dictionary<Pager2, CryptoTransactionState>? ForCrypto;
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

        public Size GetTotal32BitsMappedSize()
        {
            if (For32Bits == null)
                return new Size(0, SizeUnit.Bytes);
            var result = 0L;
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var state in For32Bits)
            {
                result += state.Value.TotalLoadedSize;
            }
            return new Size(result, SizeUnit.Bytes);
        }
    }
    
    public class State: IDisposable
    {
        public readonly Pager2 Pager;
        
        /// <summary>
        /// For the duration of the transaction that created this state, we *must*
        /// hold a hard reference to the previous state(s) to ensure that any pointer
        /// that we _already_ got is valid.
        ///
        /// This is cleared upon committing the transaction state to the global state  
        /// </summary>
        private State? _previous;
        private bool _fileOwnershipMoved;

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
