using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using Sparrow;
using Sparrow.Platform.Win32;
using Sparrow.Utils;
using Voron.Data;
using Voron.Data.Tables;
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
        public long Size;
        public bool IgnoreFree;
        public ulong Checksum;
    }

    public unsafe class CryptoPager : AbstractPager
    {
        public AbstractPager Inner { get; }
        private readonly byte[] _masterKey;
        private readonly byte[] _context;
        private const ulong MacLen = 16;
        private readonly ReaderWriterLockSlim _readerWriterLock = new ReaderWriterLockSlim();

        public override long TotalAllocationSize => Inner.TotalAllocationSize;
        public override long NumberOfAllocatedPages => Inner.NumberOfAllocatedPages;
        public override string FileName => Inner.FileName;
        public override PagerState PagerState => Inner.PagerState;
        public override StorageEnvironmentOptions Options => Inner.Options;
        public new bool Disposed { get; private set; }
        public new uint UniquePhysicalDriveId;

        public CryptoPager(AbstractPager inner) : base(inner.Options, inner.UsePageProtection)
        {

            Inner = inner;
            _masterKey = inner.Options.MasterKey;
            _context = Sodium.Context;

            Disposed = false;
            UniquePhysicalDriveId = Inner.UniquePhysicalDriveId;
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
            var buffer = GetNewBufferAndAddToTx(pageNumber, state, size);

            return buffer.Pointer;
        }

        public override byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            _readerWriterLock.EnterReadLock();
            try
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

                buffer = GetNewBufferAndAddToTx(pageNumber, state, size);

                Memory.Copy(buffer.Pointer, pagePointer, buffer.Size);

                DecryptPage((PageHeader*)buffer.Pointer);

                buffer.Checksum = Hashing.XXHash64.Calculate(buffer.Pointer, (ulong)buffer.Size);

                return buffer.Pointer;
            }
            finally
            {
                _readerWriterLock.ExitReadLock();
            }
        }

        public void BreakLargeAllocationToSeparatePages(IPagerLevelTransactionState tx, long pageNumber)
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
                    IgnoreFree = true,
                    Pointer = encBuffer.Pointer + i * Constants.Storage.PageSize,
                    Size = Constants.Storage.PageSize
                };
            }

            encBuffer.Size = Constants.Storage.PageSize;
        }

        public EncryptionBuffer GetNewBufferAndAddToTx(long pageNumber, CryptoTransactionState state, int size)
        {
            var buffer = new EncryptionBuffer
            {
                Pointer = Win32MemoryProtectMethods.VirtualAlloc(null, (UIntPtr)size, Win32MemoryProtectMethods.AllocationType.COMMIT, Win32MemoryProtectMethods.MemoryProtection.READWRITE),
                Size = size
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

            _readerWriterLock.EnterWriteLock();
            try
            {
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
            finally
            {
                _readerWriterLock.ExitWriteLock();
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

            if (tx.IsWriteTransaction)
            {
                foreach (var buffer in state.LoadedBuffers)
                {
                    Sodium.ZeroMemory(buffer.Value.Pointer, buffer.Value.Size);
                }
            }
            // We can't free in the same loop above because we might free a page which is a start of a seperated section, thus freeing the entire section. 
            // In that case, we'll have access violation starting from the second page in the section.
            foreach (var buffer in state.LoadedBuffers)
            {
                if (buffer.Value.IgnoreFree)
                    continue;
                Win32MemoryProtectMethods.VirtualFree(buffer.Value.Pointer, UIntPtr.Zero, Win32MemoryProtectMethods.FreeType.MEM_RELEASE);
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
            if (Disposed)
                return;

            Disposed = true;
            Inner.Dispose();
        }

        ~CryptoPager()
        {
            Dispose();
        }

        public override I4KbBatchWrites BatchWriter()
        {
            return new Crypto4KbBatchWrites(this);
        }

        private class Crypto4KbBatchWrites : I4KbBatchWrites
        {
            private readonly CryptoPager _parent;
            private PagerState _pagerState;

            public Crypto4KbBatchWrites(CryptoPager pager)
            {
                _parent = pager;
                _pagerState = _parent.Inner.GetPagerStateAndAddRefAtomically();
            }

            public void Write(long posBy4Kbs, int numberOf4Kbs, byte* source)
            {
                const int pageSizeTo4KbRatio = (Constants.Storage.PageSize / (4 * Constants.Size.Kilobyte));
                var pageNumber = posBy4Kbs / pageSizeTo4KbRatio;
                var offsetBy4Kb = posBy4Kbs % pageSizeTo4KbRatio;
                var numberOfPages = numberOf4Kbs / pageSizeTo4KbRatio;
                if (posBy4Kbs % pageSizeTo4KbRatio != 0 ||
                    numberOf4Kbs % pageSizeTo4KbRatio != 0)
                    numberOfPages++;

                var newPagerState = _parent.Inner.EnsureContinuous(pageNumber, numberOfPages);
                if (newPagerState != null)
                {
                    _pagerState.Release();
                    newPagerState.AddRef();
                    _pagerState = newPagerState;
                }

                var toWrite = numberOf4Kbs * 4 * Constants.Size.Kilobyte;
                byte* destination = _parent.Inner.AcquirePagePointer(null, pageNumber, _pagerState)
                                    + (offsetBy4Kb * 4 * Constants.Size.Kilobyte);

                _parent.UnprotectPageRange(destination, (ulong)toWrite);
                
                Memory.Copy(destination, source, toWrite);

                _parent.ProtectPageRange(destination, (ulong)toWrite);
            }

            public void Dispose()
            {
                _pagerState.Release();
            }
        }
    }
}