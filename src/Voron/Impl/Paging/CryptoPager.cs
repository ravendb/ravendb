using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Sparrow.Utils;
using Voron.Data;
using Constants = Voron.Global.Constants;

namespace Voron.Impl.Paging
{
    public class CryptoTransactionState : IEnumerable<KeyValuePair<long, EncryptionBuffer>>
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
        public bool TryGetValue(long i, out EncryptionBuffer value)
        {
            return _loadedBuffers.TryGetValue(i, out value);
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

    public unsafe class EncryptionBuffer
    {
        public EncryptionBuffer()
        {
            Generation = EncryptionBuffersPool.Instance.Generation;
        }

        public static readonly UIntPtr HashSize = Sodium.crypto_generichash_bytes();
        public static readonly int HashSizeInt = (int)Sodium.crypto_generichash_bytes();
        public byte* Pointer;
        public long Size;
        public long? OriginalSize;
        public bool Modified;
        public NativeMemory.ThreadStats AllocatingThread;
        public long Generation;
        public bool SkipOnTxCommit;
    }

    public sealed unsafe class CryptoPager : AbstractPager
    {
        private static readonly byte[] Context = Encodings.Utf8.GetBytes("RavenDB!");

        public AbstractPager Inner { get; }
        private readonly byte[] _masterKey;
        private const ulong MacLen = 16;

        public override long TotalAllocationSize => Inner.TotalAllocationSize;

        public override long NumberOfAllocatedPages => Inner.NumberOfAllocatedPages;

        public CryptoPager(AbstractPager inner) : base(inner.Options, inner.UsePageProtection)
        {
            if (inner.Options.Encryption.IsEnabled == false)
                throw new InvalidOperationException("Cannot use CryptoPager if IsEnabled is false (no key defined)");

            Inner = inner;
            _masterKey = inner.Options.Encryption.MasterKey;

            UniquePhysicalDriveId = Inner.UniquePhysicalDriveId;
            FileName = inner.FileName;
            _pagerState = inner.PagerState;
            inner.PagerStateChanged += state => _pagerState = state;

            inner.Options.TrackCryptoPager(this);
        }

        protected override string GetSourceName()
        {
            return "Crypto " + Inner;
        }

        public override void Sync(long totalUnsynced)
        {
            Inner.Sync(totalUnsynced);
        }

        protected internal override PagerState AllocateMorePages(long newLength)
        {
            return Inner.AllocateMorePages(newLength);
        }

        public override string ToString()
        {
            return GetSourceName();
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            Inner.ReleaseAllocationInfo(baseAddress, size);
        }

        public override void DiscardPages(long pageNumber, int numberOfPages)
        {
            Inner.DiscardPages(pageNumber, numberOfPages);
        }

        public override void DiscardWholeFile()
        {
            Inner.DiscardWholeFile();
        }

        protected override bool CanPrefetchQuery()
        {
            return Inner.CanPrefetch.Value;
        }

        public override int CopyPage(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState)
        {
            return Inner.CopyPage(destwI4KbBatchWrites, p, pagerState);
        }

        internal override void ProtectPageRange(byte* start, ulong size, bool force = false)
        {
            Inner.ProtectPageRange(start, size, force);
        }

        internal override void UnprotectPageRange(byte* start, ulong size, bool force = false)
        {
            Inner.UnprotectPageRange(start, size, force);
        }

        public override byte* AcquirePagePointerForNewPage(IPagerLevelTransactionState tx, long pageNumber, int numberOfPages, PagerState pagerState = null)
        {
            var state = GetTransactionState(tx);
            var size = numberOfPages * Constants.Storage.PageSize;

            if (state.TryGetValue(pageNumber, out var buffer))
            {
                if (size == buffer.Size)
                {
                    Sodium.sodium_memzero(buffer.Pointer, (UIntPtr)size);

                    buffer.SkipOnTxCommit = false;
                    return buffer.Pointer;
                }

                ReturnBuffer(buffer);
            }

            // allocate new buffer
            buffer = GetBufferAndAddToTxState(pageNumber, state, numberOfPages);
            buffer.Modified = true;

            return buffer.Pointer;
        }

        public override byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            var state = GetTransactionState(tx);

            if (state.TryGetValue(pageNumber, out var buffer))
                return buffer.Pointer;

            var pagePointer = Inner.AcquirePagePointerWithOverflowHandling(tx, pageNumber, pagerState);

            var pageHeader = (PageHeader*)pagePointer;

            int numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfPages(pageHeader);

            buffer = GetBufferAndAddToTxState(pageNumber, state, numberOfPages);

            Memory.Copy(buffer.Pointer, pagePointer, buffer.Size);

            DecryptPage((PageHeader*)buffer.Pointer);

            return buffer.Pointer;
        }

        public override void BreakLargeAllocationToSeparatePages(IPagerLevelTransactionState tx, long pageNumber)
        {
            if (tx == null)
                throw new NotSupportedException("Cannot use crypto pager without a transaction");

            var state = GetTransactionState(tx);

            if (state.TryGetValue(pageNumber, out var encBuffer) == false)
                throw new InvalidOperationException("Tried to break buffer that wasn't allocated in this tx");

            for (int i = 1; i < encBuffer.Size / Constants.Storage.PageSize; i++)
            {
                var buffer = new EncryptionBuffer
                {
                    Pointer = encBuffer.Pointer + i * Constants.Storage.PageSize,
                    Size = Constants.Storage.PageSize,
                    OriginalSize = 0,
                    AllocatingThread = encBuffer.AllocatingThread
                };

                // when we commit
                // the tx, the pager will realize that we need to write this page
                buffer.Modified = true;

                state[pageNumber + i] = buffer;
            }

            encBuffer.OriginalSize = encBuffer.Size;
            encBuffer.Size = Constants.Storage.PageSize;

            // here we _intentionally_ don't modify the hash of the page, even though its size was
            // changed, because we need the pager to recognize that it was modified on tx commit
            // encBuffer.Hash = remains the same
        }

        private EncryptionBuffer GetBufferAndAddToTxState(long pageNumber, CryptoTransactionState state, int numberOfPages)
        {
            var ptr = EncryptionBuffersPool.Instance.Get(numberOfPages, out var size, out var thread);

            var buffer = new EncryptionBuffer
            {
                Size = size,
                Pointer = ptr,
                AllocatingThread = thread
            };

            state[pageNumber] = buffer;
            return buffer;
        }

        private CryptoTransactionState GetTransactionState(IPagerLevelTransactionState tx)
        {
            if (tx == null)
                throw new NotSupportedException("Cannot use crypto pager without a transaction");

            CryptoTransactionState transactionState;
            if (tx.CryptoPagerTransactionState == null)
            {
                transactionState = new CryptoTransactionState();
                tx.CryptoPagerTransactionState = new Dictionary<AbstractPager, CryptoTransactionState>
                {
                    {this, transactionState}
                };
                tx.OnDispose += TxOnDispose;
                tx.BeforeCommitFinalization += TxOnCommit;
                return transactionState;
            }

            if (tx.CryptoPagerTransactionState.TryGetValue(this, out transactionState) == false)
            {
                transactionState = new CryptoTransactionState();
                tx.CryptoPagerTransactionState[this] = transactionState;
                tx.OnDispose += TxOnDispose;
                tx.BeforeCommitFinalization += TxOnCommit;
            }
            return transactionState;
        }

        private void TxOnCommit(IPagerLevelTransactionState tx)
        {
            if (tx.IsWriteTransaction == false)
                return;

            if (tx.CryptoPagerTransactionState == null)
                return;

            if (tx.CryptoPagerTransactionState.TryGetValue(this, out var state) == false)
                return;

            foreach (var buffer in state)
            {
                if (buffer.Value.SkipOnTxCommit)
                    continue;

                if (buffer.Value.Modified == false)
                    continue; // No modification

                // Encrypt the local buffer, then copy the encrypted value to the pager
                var pageHeader = (PageHeader*)buffer.Value.Pointer;
                var dataSize = EncryptPage(pageHeader);
                var numPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(dataSize);

                Inner.EnsureContinuous(buffer.Key, numPages);
                Inner.EnsureMapped(tx, buffer.Key, numPages);

                var pagePointer = Inner.AcquirePagePointer(tx, buffer.Key);

                Memory.Copy(pagePointer, buffer.Value.Pointer, dataSize);
            }
        }

        private void TxOnDispose(IPagerLevelTransactionState tx)
        {
            if (tx.CryptoPagerTransactionState == null)
                return;

            if (tx.CryptoPagerTransactionState.TryGetValue(this, out var state) == false)
                return;

            tx.CryptoPagerTransactionState.Remove(this);

            foreach (var buffer in state)
            {
                if (buffer.Value.OriginalSize != null && buffer.Value.OriginalSize == 0)
                {
                    // Pages that are marked with OriginalSize = 0 were separated from a larger allocation, we cannot free them directly.
                    // The first page of the section will be returned and when it will be freed, all the other parts will be freed as well.
                    continue;
                }

                ReturnBuffer(buffer.Value);
            }
        }

        private void ReturnBuffer(EncryptionBuffer buffer)
        {
            if (buffer.OriginalSize != null && buffer.OriginalSize != 0)
            {
                // First page of a separated section, returned with its original size.
                EncryptionBuffersPool.Instance.Return(buffer.Pointer, (int)buffer.OriginalSize, buffer.AllocatingThread, buffer.Generation);
            }
            else
            {
                // Normal buffers
                EncryptionBuffersPool.Instance.Return(buffer.Pointer, buffer.Size, buffer.AllocatingThread, buffer.Generation);
            }
        }

        private int EncryptPage(PageHeader* page)
        {
            var num = page->PageNumber;
            var destination = (byte*)page;
            var subKeyLen = Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
            var subKey = stackalloc byte[(int)subKeyLen];
            fixed (byte* ctx = Context)
            fixed (byte* mk = _masterKey)
            {
                if (Sodium.crypto_kdf_derive_from_key(subKey, subKeyLen, (ulong)num, ctx, mk) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");

                var dataSize = VirtualPagerLegacyExtensions.GetNumberOfPages(page) * Constants.Storage.PageSize;

                var npub = (byte*)page + PageHeader.NonceOffset;
                // here we generate 128(!) bits of random data, but xchacha20poly1305 needs
                // 192 bits, we go to backward from the radnom nonce to  get more bits that
                // are not really random for the algorithm.
                Sodium.randombytes_buf(npub, (UIntPtr)(PageHeader.MacOffset - PageHeader.NonceOffset));

                ulong macLen = MacLen;
                var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(
                    destination + PageHeader.SizeOf,
                    destination + PageHeader.MacOffset,
                    &macLen,
                    (byte*)page + PageHeader.SizeOf,
                    (ulong)dataSize - PageHeader.SizeOf,
                    (byte*)page,
                    (ulong)PageHeader.NonceOffset,
                    null,
                    // got back a bit to allow for 192 bits nonce, even if the first
                    // 8 bytes aren't really random, the last 128 bits are securely
                    // radnom
                    (byte*)page + PageHeader.NonceOffset - sizeof(long),
                    subKey
                );
                Debug.Assert(macLen == MacLen);

                if (rc != 0)
                    throw new InvalidOperationException($"Unable to encrypt page {num}, rc={rc}");

                return dataSize;
            }
        }

        private void DecryptPage(PageHeader* page)
        {
            var num = page->PageNumber;

            var destination = (byte*)page;
            var subKeyLen = Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
            var subKey = stackalloc byte[(int)subKeyLen];
            fixed (byte* ctx = Context)
            fixed (byte* mk = _masterKey)
            {
                if (Sodium.crypto_kdf_derive_from_key(subKey, subKeyLen, (ulong)num, ctx, mk) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");

                var dataSize = (ulong)VirtualPagerLegacyExtensions.GetNumberOfPages(page) * Constants.Storage.PageSize;
                var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(
                    destination + PageHeader.SizeOf,
                    null,
                    (byte*)page + PageHeader.SizeOf,
                    dataSize - PageHeader.SizeOf,
                    (byte*)page + PageHeader.MacOffset,
                    (byte*)page,
                    (ulong)PageHeader.NonceOffset,
                    // we need to go 8 bytes before the nonce to get where
                    // the full nonce (fixed 8 bytes + random 16 bytes).
                    (byte*)page + PageHeader.NonceOffset - sizeof(long),
                    subKey
                );
                if (rc != 0)
                    throw new InvalidOperationException($"Unable to decrypt page {num}, rc={rc}");
            }
        }

        protected override void DisposeInternal()
        {
            Inner.Options.UntrackCryptoPager(this);
            Inner.Dispose();
        }

        public override I4KbBatchWrites BatchWriter()
        {
            return Inner.BatchWriter();
        }

        public override byte* AcquireRawPagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            return Inner.AcquireRawPagePointer(tx, pageNumber, pagerState);
        }
    }
}
