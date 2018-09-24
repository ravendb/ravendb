using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Utils;
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
        public static readonly UIntPtr HashSize = Sodium.crypto_generichash_bytes();
        public static readonly int HashSizeInt = (int)Sodium.crypto_generichash_bytes();
        public byte* Pointer;
        public int Size;
        public int? OriginalSize;
        public byte* Hash;
        public NativeMemory.ThreadStats AllocatingThread;
        public bool SkipOnTxCommit;
    }

    public sealed unsafe class CryptoPager : AbstractPager
    {
        private static readonly byte[] Context = Encodings.Utf8.GetBytes("RavenDB!");

            
        public AbstractPager Inner { get; }
        private readonly EncryptionBuffersPool _encryptionBuffersPool;
        private readonly byte[] _masterKey;
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

        public override void MaybePrefetchMemory<T>(T pagesToPrefetch) 
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
            var state = GetTransactionState(tx);
            var size = numberOfPages * Constants.Storage.PageSize;

            if (state.LoadedBuffers.TryGetValue(pageNumber, out var buffer))
            {
                if (size == buffer.Size)
                {
                    Sodium.sodium_memzero(buffer.Pointer, (UIntPtr)size);
                    Sodium.sodium_memzero(buffer.Hash, (UIntPtr)EncryptionBuffer.HashSizeInt);

                    buffer.SkipOnTxCommit = false;
                    return buffer.Pointer;
                }

                ReturnBuffer(buffer);
            }

            // allocate new buffer
            buffer = GetBufferAndAddToTxState(pageNumber, state, size);

            return buffer.Pointer;
        }

        public override byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            var state = GetTransactionState(tx);

            if (state.LoadedBuffers.TryGetValue(pageNumber, out var buffer))
                return buffer.Pointer;

            var pagePointer = Inner.AcquirePagePointerWithOverflowHandling(tx, pageNumber, pagerState);

            var pageHeader = (PageHeader*)pagePointer;

            var size = GetNumberOfPages(pageHeader) * Constants.Storage.PageSize;

            buffer = GetBufferAndAddToTxState(pageNumber, state, size);

            Memory.Copy(buffer.Pointer, pagePointer, buffer.Size);

            DecryptPage((PageHeader*)buffer.Pointer);

            if(Sodium.crypto_generichash(buffer.Hash, EncryptionBuffer.HashSize, buffer.Pointer, (ulong)buffer.Size, null, UIntPtr.Zero) != 0)
                ThrowInvalidHash();
            
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
                var buffer = new EncryptionBuffer
                {
                    Pointer = encBuffer.Pointer + i * Constants.Storage.PageSize,
                    Size = Constants.Storage.PageSize,
                    OriginalSize = 0,
                };

                buffer.Hash = _encryptionBuffersPool.Get(EncryptionBuffer.HashSizeInt, out var thread);
                buffer.AllocatingThread = thread;

                // here we _intentionally_ copy the old hash from the large page, so when we commit
                // the tx, the pager will realize that we need to write this page
                Memory.Copy(buffer.Hash, encBuffer.Hash, EncryptionBuffer.HashSizeInt);

                state.LoadedBuffers[pageNumber + i] = buffer;
            }

            encBuffer.OriginalSize = encBuffer.Size;
            encBuffer.Size = Constants.Storage.PageSize;

            // here we _intentionally_ don't modify the hash of the page, even though its size was 
            // changed, because we need the pager to recognize that it was modified on tx commit
            // encBuffer.Hash = remains the same
        }

        private EncryptionBuffer GetBufferAndAddToTxState(long pageNumber, CryptoTransactionState state, int size)
        {
            var ptr = _encryptionBuffersPool.Get(size, out var thread);
            var hash = _encryptionBuffersPool.Get(EncryptionBuffer.HashSizeInt, out thread);
            
            var buffer = new EncryptionBuffer
            {
                Size = size,
                Pointer = ptr,
                Hash = hash,
                AllocatingThread = thread
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

            if (tx.CryptoPagerTransactionState.TryGetValue(this, out var state) == false)
                return;

            var pageHash = stackalloc byte[EncryptionBuffer.HashSizeInt];
            foreach (var buffer in state.LoadedBuffers)
            {
                if (buffer.Value.SkipOnTxCommit)
                    continue;

                if(Sodium.crypto_generichash(pageHash, EncryptionBuffer.HashSize, buffer.Value.Pointer, (ulong)buffer.Value.Size, null, UIntPtr.Zero) != 0)
                    ThrowInvalidHash();

                if (Sodium.sodium_memcmp(pageHash, buffer.Value.Hash, EncryptionBuffer.HashSize) == 0)
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

        private static void ThrowInvalidHash([CallerMemberName] string caller = null)
        {
            throw new InvalidOperationException($"Unable to compute hash for buffer in " + caller);
        }

        private void TxOnDispose(IPagerLevelTransactionState tx)
        {
            if (tx.CryptoPagerTransactionState == null)
                return;

            if (tx.CryptoPagerTransactionState.TryGetValue(this, out var state) == false)
                return;

            tx.CryptoPagerTransactionState.Remove(this);
            
            foreach (var buffer in state.LoadedBuffers)
            {
                if (buffer.Value.OriginalSize != null && buffer.Value.OriginalSize == 0)
                {
                    // Pages that are marked with OriginalSize = 0 were separated from a larger allocation, we cannot free them directly.
                    // The first page of the section will be returned and when it will be freed, all the other parts will be freed as well.
                    // We still need to return the buffer allocated for the hash
                    _encryptionBuffersPool.Return(buffer.Value.Hash, EncryptionBuffer.HashSizeInt, buffer.Value.AllocatingThread);

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
                _encryptionBuffersPool.Return(buffer.Pointer, (int)buffer.OriginalSize, buffer.AllocatingThread);
                _encryptionBuffersPool.Return(buffer.Hash, EncryptionBuffer.HashSizeInt, buffer.AllocatingThread);
            }
            else
            {
                // Normal buffers
                _encryptionBuffersPool.Return(buffer.Pointer, buffer.Size, buffer.AllocatingThread);
                _encryptionBuffersPool.Return(buffer.Hash, EncryptionBuffer.HashSizeInt, buffer.AllocatingThread);
            }
        }

        private int EncryptPage(PageHeader* page)
        {
            var num = page->PageNumber;
            var destination = (byte*)page;
            var subKeyLen = Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
            var subKey = stackalloc byte[(int)subKeyLen ];
            fixed (byte* ctx = Context)
            fixed (byte* mk = _masterKey)
            {
                if (Sodium.crypto_kdf_derive_from_key(subKey, subKeyLen, (ulong)num, ctx, mk) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");

                var dataSize = GetNumberOfPages(page) * Constants.Storage.PageSize;

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
            var subKey = stackalloc byte[(int)subKeyLen ];
            fixed (byte* ctx = Context)
            fixed (byte* mk = _masterKey)
            {
                if (Sodium.crypto_kdf_derive_from_key(subKey, subKeyLen , (ulong)num, ctx, mk) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");

                var dataSize = (ulong)GetNumberOfPages(page) * Constants.Storage.PageSize;
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
