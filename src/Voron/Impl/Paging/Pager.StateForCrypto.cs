#nullable enable
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Sparrow.Utils;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
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
        public bool TryGetValue(long pageNumber, [MaybeNullWhen(false)] out EncryptionBuffer value)
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
    
    
    public sealed class EncryptionBuffer(EncryptionBuffersPool pool, NativeMemory.ThreadStats thread, byte* ptr, long size)
    {
        public readonly long Generation = pool.Generation;
        public long Size = size;
        public byte* Pointer = ptr;
        public NativeMemory.ThreadStats AllocatingThread = thread;
        public long OriginalSize;
        
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
}
