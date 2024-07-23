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
using Voron.Global;
using Voron.Impl.Scratch;
using Voron.Platform.Win32;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
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
        public void RemoveBuffer(Pager pager, long pageNumber)
        {
            if (_loadedBuffers.Remove(pageNumber, out var buffer))
            {
                pager._encryptionBuffersPool.Return(buffer.Pointer, buffer.Size, buffer.AllocatingThread, buffer.Generation);
            }
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
    
    public sealed class EncryptionBuffer(EncryptionBuffersPool pool)
    {
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
        public Dictionary<Pager, TxStateFor32Bits>? For32Bits;
        public Dictionary<Pager, CryptoTransactionState>? ForCrypto;
        public bool IsWriteTransaction;
        
        /// <summary>
        /// These are events because we may have a single transaction deal
        /// with multiple pagers 
        /// </summary>
        public event TxStateDelegate OnDispose;
        public event TxStateDelegate OnBeforeCommitFinalization;
        
        public delegate void TxStateDelegate(StorageEnvironment environment, ref State dataPagerState, ref PagerTransactionState txState);

        public void InvokeBeforeCommitFinalization(StorageEnvironment environment, ref State dataPagerState, ref PagerTransactionState txState) => OnBeforeCommitFinalization?.Invoke(environment, ref dataPagerState,  ref txState);
        public void InvokeDispose(StorageEnvironment environment, ref State dataPagerState,ref PagerTransactionState txState) => OnDispose?.Invoke(environment, ref dataPagerState, ref txState);

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
        
        
        public Size AdditionalMemoryUsageSize
        {
            get
            {
                
                var cryptoTransactionStates = ForCrypto;
                if( cryptoTransactionStates== null)
                    return Size.Zero;


                long total = 0;
                foreach (var state in cryptoTransactionStates.Values)
                {
                    total += state.TotalCryptoBufferSize;
                }
                return new Size(total, SizeUnit.Bytes);
            }
        }
    }
    
    public class State: IDisposable
    {
        public readonly Pager Pager;
        
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

        public State(Pager pager, byte* baseAddress, long totalAllocatedSize, void* handle)
        {
            BaseAddress = baseAddress;
            TotalAllocatedSize = totalAllocatedSize;
            NumberOfAllocatedPages = totalAllocatedSize / Constants.Storage.PageSize;
            Handle = handle;
       
            Pager = pager;
            _previous = null;
            WeakSelf = new WeakReference<State>(this);
        }


        public byte* BaseAddress;
        public long NumberOfAllocatedPages;
        public long TotalAllocatedSize;

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
                NativeMemory.UnregisterFileMapping(Pager.FileName, (nint)BaseAddress, TotalAllocatedSize);
                
                if (rc != PalFlags.FailCodes.Success)
                {
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to close data pager for: {Pager.FileName}");
                }
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
    }
}
