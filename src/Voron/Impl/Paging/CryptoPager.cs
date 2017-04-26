using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Platform.Win32;
using Voron.Data;
using Voron.Global;

namespace Voron.Impl.Paging
{
    public class CryptoTransactionState
    {
        public Dictionary<long, EncryptionBuffer> LoadedBuffers = new Dictionary<long, EncryptionBuffer>();
    }

    public unsafe class EncryptionBuffer
    {
        public byte* Pointer;
        public int Size;
        public int? OriginalSize;
        public ulong Checksum;
    }

    public unsafe class CryptoPager : AbstractPager
    {
        public AbstractPager Inner { get; }
        private readonly EncryptionBuffersPool _encryptionBuffersPool;
        private readonly byte[] _masterKey;
        private readonly byte[] _context;
        private const ulong MacLen = 16;

        public override long TotalAllocationSize => Inner.TotalAllocationSize;
        public override long NumberOfAllocatedPages => Inner.NumberOfAllocatedPages;

        public CryptoPager(AbstractPager inner) : base(inner.Options, inner.UsePageProtection)
        {
            if (inner.Options.EncryptionEnabled == false)
                throw new InvalidOperationException("Cannot use CryptoPager if EncryptionEnabled is false (no key defined)");

            Inner = inner;
            _encryptionBuffersPool = new EncryptionBuffersPool();
            _masterKey = inner.Options.MasterKey;
            _context = Sodium.Context;

            UniquePhysicalDriveId = Inner.UniquePhysicalDriveId;
            FileName = inner.FileName;
            _pagerState = inner.PagerState;
            inner.PagerStateChanged += state => _pagerState = state;
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

        public override void TryPrefetchingWholeFile()
        {
            Inner.TryPrefetchingWholeFile();
        }

        public override void MaybePrefetchMemory(List<long> pagesToPrefetch)
        {
            Inner.MaybePrefetchMemory(pagesToPrefetch);
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

        private static int GetNumberOfPages(PageHeader* header)
        {
            if ((header->Flags & PageFlags.Overflow) != PageFlags.Overflow)
                return 1;

            var overflowSize = header->OverflowSize + Constants.Tree.PageHeaderSize;
            return checked((overflowSize / Constants.Storage.PageSize) + (overflowSize % Constants.Storage.PageSize == 0 ? 0 : 1));
        }

        public override byte* AcquirePagePointerForNewPage(IPagerLevelTransactionState tx, long pageNumber, int numberOfPages, PagerState pagerState = null)
        {
            // New page -> no need to read page, just allocate a new buffer
            var state = GetTransactionState(tx);
            var size = numberOfPages * Constants.Storage.PageSize;
            var buffer = GetBufferAndAddToTxState(pageNumber, state, size);

            return buffer.Pointer;
        }

        public override byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            var state = GetTransactionState(tx);

            EncryptionBuffer buffer;
            if (state.LoadedBuffers.TryGetValue(pageNumber, out buffer))
            {
                return buffer.Pointer;
            }

            var pagePointer = Inner.AcquirePagePointer(tx, pageNumber, pagerState);

            var pageHeader = (PageHeader*)pagePointer;

            var size = GetNumberOfPages(pageHeader) * Constants.Storage.PageSize;

            buffer = GetBufferAndAddToTxState(pageNumber, state, size);

            Memory.Copy(buffer.Pointer, pagePointer, buffer.Size);

            DecryptPage((PageHeader*)buffer.Pointer);

            buffer.Checksum = Hashing.XXHash64.Calculate(buffer.Pointer, (ulong)buffer.Size);

            return buffer.Pointer;

        }

        public override void BreakLargeAllocationToSeparatePages(IPagerLevelTransactionState tx, long pageNumber)
        {
            if (tx == null)
                throw new NotSupportedException("Cannot use crypto pager without a transaction");

            var state = GetTransactionState(tx);

            if (state.LoadedBuffers.TryGetValue(pageNumber, out var encBuffer) == false)
                throw new InvalidOperationException("Tried to break buffer that wasn't allocated in this tx");

            for (int i = 1; i < encBuffer.Size / Constants.Storage.PageSize; i++)
            {
                state.LoadedBuffers[pageNumber + i] = new EncryptionBuffer
                {
                    Pointer = encBuffer.Pointer + i * Constants.Storage.PageSize,
                    Size = Constants.Storage.PageSize,
                    OriginalSize = 0
                };
            }

            encBuffer.OriginalSize = encBuffer.Size;
            encBuffer.Size = Constants.Storage.PageSize;
        }

        private EncryptionBuffer GetBufferAndAddToTxState(long pageNumber, CryptoTransactionState state, int size)
        {
            var buffer = new EncryptionBuffer
            {
                Size = size,
                Pointer = _encryptionBuffersPool.Get(size)
            };
            state.LoadedBuffers[pageNumber] = buffer;
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

            CryptoTransactionState state;
            if (tx.CryptoPagerTransactionState.TryGetValue(this, out state) == false)
                return;

            foreach (var buffer in state.LoadedBuffers)
            {
                var checksum = Hashing.XXHash64.Calculate(buffer.Value.Pointer, (ulong)buffer.Value.Size);
                if (checksum == buffer.Value.Checksum)
                    continue; // No modification

                // Encrypt the local buffer, then copy the encrypted value to the pager
                var pageHeader = (PageHeader*)buffer.Value.Pointer;
                EncryptPage(pageHeader);

                var pagePointer = Inner.AcquirePagePointer(null, buffer.Key);

                Memory.Copy(pagePointer, buffer.Value.Pointer, buffer.Value.Size);
            }

        }

        private void TxOnDispose(IPagerLevelTransactionState tx)
        {
            if (tx.CryptoPagerTransactionState == null)
                return;

            CryptoTransactionState state;
            if (tx.CryptoPagerTransactionState.TryGetValue(this, out state) == false)
                return;

            tx.CryptoPagerTransactionState.Remove(this);
            
            foreach (var buffer in state.LoadedBuffers)
            {
                if (buffer.Value.OriginalSize != null && buffer.Value.OriginalSize == 0)
                {
                    // Pages that are marked with OriginalSize = 0 were seperated from a larger allocation, we cannot free them directly.
                    // The first page of the section will be returned and when it will be freed, all the other parts will be freed as well.
                    continue;
                }

                if (buffer.Value.OriginalSize != null && buffer.Value.OriginalSize != 0)
                {
                    // First page of a seperated section, returned with its original size.
                    _encryptionBuffersPool.Return(buffer.Value.Pointer, (int)buffer.Value.OriginalSize);
                    continue;
                }

                // Normal buffers
                _encryptionBuffersPool.Return(buffer.Value.Pointer, buffer.Value.Size);
            }
        }

        private void EncryptPage(PageHeader* page)
        {
            var num = page->PageNumber;
            var destination = (byte*)page;
            var subKey = stackalloc byte[32];
            fixed (byte* ctx = _context)
            fixed (byte* mk = _masterKey)
            {
                if (Sodium.crypto_kdf_derive_from_key(subKey, 32, num, ctx, mk) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");

                var dataSize = (ulong)GetNumberOfPages(page) * Constants.Storage.PageSize;

                var npub = (byte*)page + PageHeader.NonceOffset;
                if (*(long*)npub == 0)
                    Sodium.randombytes_buf(npub, sizeof(long));
                else
                    *(long*)npub = *(long*)npub + 1;

                ulong macLen = MacLen;
                var rc = Sodium.crypto_aead_chacha20poly1305_encrypt_detached(
                    destination + PageHeader.SizeOf,
                    destination + PageHeader.MacOffset,
                    &macLen,
                    (byte*)page + PageHeader.SizeOf,
                    dataSize - PageHeader.SizeOf,
                    (byte*)page,
                    (ulong)PageHeader.NonceOffset,
                    null,
                    npub,
                    subKey
                );
                Debug.Assert(macLen == MacLen);

                if (rc != 0)
                    throw new InvalidOperationException($"Unable to encrypt page {num}, rc={rc}");
            }
        }

        private void DecryptPage(PageHeader* page)
        {
            var num = page->PageNumber;

            var destination = (byte*)page;
            var subKey = stackalloc byte[32];
            fixed (byte* ctx = _context)
            fixed (byte* mk = _masterKey)
            {
                if (Sodium.crypto_kdf_derive_from_key(subKey, 32, num, ctx, mk) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");

                var dataSize = (ulong)GetNumberOfPages(page) * Constants.Storage.PageSize;
                var rc = Sodium.crypto_aead_chacha20poly1305_decrypt_detached(
                    destination + PageHeader.SizeOf,
                    null,
                    (byte*)page + PageHeader.SizeOf,
                    dataSize - PageHeader.SizeOf,
                    (byte*)page + PageHeader.MacOffset,
                    (byte*)page,
                    (ulong)PageHeader.NonceOffset,
                    (byte*)page + PageHeader.NonceOffset,
                    subKey
                );
                if (rc != 0)
                    throw new InvalidOperationException($"Unable to decrypt page {num}, rc={rc}");
            }
        }

        public override void Dispose()
        {
            Inner.Dispose();
            _encryptionBuffersPool.Dispose();
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