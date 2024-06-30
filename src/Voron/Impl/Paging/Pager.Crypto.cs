using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager2
{
    public static class Crypto
    {
        public static byte* AcquirePagePointerForNewPage(Pager2 pager, long pageNumber, int numberOfPages, State state, ref PagerTransactionState txState)
        {
            Debug.Assert(pager.Options.Encryption.IsEnabled);

            var cryptoState = GetTransactionState(pager, ref txState);
            var size = numberOfPages * Constants.Storage.PageSize;
            
            if (cryptoState.TryGetValue(pageNumber, out var buffer))
            {
                if (size == buffer.Size)
                {
                    buffer.AddRef();

                    Sodium.sodium_memzero(buffer.Pointer, (UIntPtr)size);

                    buffer.SkipOnTxCommit = false;
                    return buffer.Pointer;
                }

                ReturnBuffer(pager, buffer);
            }

            // allocate new buffer
            buffer = GetBufferAndAddToTxState(pager, pageNumber, cryptoState, numberOfPages);
            buffer.Modified = true;

            return buffer.Pointer;
        }

        public static byte* AcquirePagePointer(Pager2 pager, State state, ref PagerTransactionState txState, long pageNumber)
        {
            Debug.Assert(pager.Options.Encryption.IsEnabled);

            var cryptoState = GetTransactionState(pager, ref txState);

            if (cryptoState.TryGetValue(pageNumber, out var buffer))
            {
                buffer.AddRef();
                return buffer.Pointer;
            }

            var pagePointer = pager.AcquireRawPagePointerWithOverflowHandling(state, ref txState, pageNumber);

            var pageHeader = (PageHeader*)pagePointer;

            int numberOfPages = Pager.GetNumberOfPages(pageHeader);

            buffer = GetBufferAndAddToTxState(pager, pageNumber, cryptoState, numberOfPages);

            var toCopy = numberOfPages * Constants.Storage.PageSize;

            AssertCopyWontExceedPagerFile(state, toCopy, pageNumber);

            Memory.Copy(buffer.Pointer, pagePointer, toCopy);

            DecryptPage(pager, (PageHeader*)buffer.Pointer);

            return buffer.Pointer;
        }
        
        [Conditional("DEBUG")]
        private static void AssertCopyWontExceedPagerFile(State state, int toCopy, long startPageNumberToCopy)
        {
            long toCopyInPages = checked(toCopy / Constants.Storage.PageSize + (toCopy % Constants.Storage.PageSize == 0 ? 0 : 1));

            if (startPageNumberToCopy + toCopyInPages > state.NumberOfAllocatedPages)
            {
                throw new InvalidOperationException(
                    $"Copying encrypted page exceeded the page file size. Number of allocated pages is {state.NumberOfAllocatedPages} while it attempted to access page {startPageNumberToCopy} and copy {toCopy} bytes");
            }
        }

        private static ReadOnlySpan<byte> Context => "RavenDB!"u8;
        private const ulong MacLen = 16;

        private static int EncryptPage(Pager2 pager, PageHeader* page)
        {
            var num = page->PageNumber;
            var destination = (byte*)page;
            var subKeyLen = Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
            var subKey = stackalloc byte[(int)subKeyLen];
            fixed (byte* ctx = Context)
            fixed (byte* mk = pager._masterKey)
            {
                if (Sodium.crypto_kdf_derive_from_key(subKey, subKeyLen, (ulong)num, ctx, mk) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");

                var dataSize = Pager.GetNumberOfPages(page) * Constants.Storage.PageSize;

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

        private static void DecryptPage(Pager2 pager,PageHeader* page)
        {
            var num = page->PageNumber;

            var destination = (byte*)page;
            var subKeyLen = Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
            var subKey = stackalloc byte[(int)subKeyLen];
            fixed (byte* ctx = Context)
            fixed (byte* mk = pager._masterKey)
            {
                if (Sodium.crypto_kdf_derive_from_key(subKey, subKeyLen, (ulong)num, ctx, mk) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");

                var dataSize = (ulong)Pager.GetNumberOfPages(page) * Constants.Storage.PageSize;
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
        
        private static EncryptionBuffer GetBufferAndAddToTxState(Pager2 pager, long pageNumber, CryptoTransactionState state, int numberOfPages)
        {
            var ptr = pager._encryptionBuffersPool.Get(numberOfPages, out var size, out var thread);
            var buffer = new EncryptionBuffer(pager._encryptionBuffersPool)
            {
                Size = size,
                OriginalSize = size,
                Pointer = ptr,
                AllocatingThread = thread
            };

            buffer.AddRef();
            state[pageNumber] = buffer;
            return buffer;
        }

        private static void ReturnBuffer(Pager2 pager, EncryptionBuffer buffer)
        {
            pager._encryptionBuffersPool.Return(buffer.Pointer, buffer.OriginalSize, buffer.AllocatingThread, buffer.Generation);
        }

        
        private static CryptoTransactionState GetTransactionState(Pager2 pager, ref PagerTransactionState txState)
        {
            txState.ForCrypto ??= new Dictionary<Pager2, CryptoTransactionState>();
            if (txState.ForCrypto.TryGetValue(pager, out var cryptoState) == false)
            {
                txState.OnDispose += TxOnDispose;
                txState.ForCrypto[pager] = cryptoState = new CryptoTransactionState();
                if (txState.IsWriteTransaction)
                {
                    txState.OnBeforeCommitFinalization += TxOnCommit;
                }
            }
            return cryptoState;
        }

        private static void TxOnCommit(Pager2 pager, State state, ref PagerTransactionState txState)
        {
            if (txState.ForCrypto?.TryGetValue(pager, out var cryptoState) != true)
                return;
            
            foreach (var buffer in cryptoState)
            {
                if (buffer.Value.SkipOnTxCommit)
                    continue;

                if (buffer.Value.Modified == false)
                {
                     
                    if (buffer.Value.ExcessStorageFromAllocationBreakup)
                        continue;// do not attempt to validate memory that we are ignoring

                    Debug_VerifyDidNotChanged(pager, state, ref txState, buffer.Key, buffer.Value);
                    continue; // No modification
                }

                // Encrypt the local buffer, then copy the encrypted value to the pager
                var pageHeader = (PageHeader*)buffer.Value.Pointer;
                var dataSize = EncryptPage(pager, pageHeader);
                var numPages = Pager.GetNumberOfOverflowPages(dataSize);

                pager.EnsureContinuous(ref state, buffer.Key, numPages);
                pager.EnsureMapped(state, ref txState, buffer.Key, numPages);

                var pagePointer = pager.AcquireRawPagePointer(state, ref txState, buffer.Key);
                Debug.Assert(pager._flags.HasFlag( Pal.OpenFileFlags.WritableMap),
                    "pager._flags.HasFlag( Pal.OpenFileFlags.WritableMap) - expected a scratch file pager, not a data pager!");
                Memory.Copy(pagePointer, buffer.Value.Pointer, dataSize);
            }
        }

        private static void TxOnDispose(Pager2 pager, State state, ref PagerTransactionState txState)
        {
            if (txState.ForCrypto?.Remove(pager, out var cryptoState) != true)
                return;

            foreach (var buffer in cryptoState)
            {
                if (CanReturnBuffer(buffer.Value))
                    continue;

                buffer.Value.ReleaseRef();

                ReturnBuffer(pager, buffer.Value);
            }
        }

        
        [Conditional("DEBUG")]
        private static void Debug_VerifyDidNotChanged(Pager2 pager, State state, ref PagerTransactionState txState, long pageNumber, EncryptionBuffer buffer)
        {
            var pagePointer = pager.AcquirePagePointerWithOverflowHandling(state, ref txState, pageNumber);
            var pageHeader = (PageHeader*)pagePointer;
            int numberOfPages = Pager.GetNumberOfPages(pageHeader);

            PageHeader* bufferPointer = ((PageHeader*)buffer.Pointer);
            if (pageHeader->PageNumber != bufferPointer->PageNumber || 
                pageHeader->OverflowSize != bufferPointer->OverflowSize)
                throw new InvalidOperationException($"The header of {pageNumber} was modified, but it was *not* changed in this transaction!");

            var toCopy = numberOfPages * Constants.Storage.PageSize;
            AssertCopyWontExceedPagerFile(state, toCopy, pageNumber);

            ulong currentHash = Hashing.XXHash64.Calculate(buffer.Pointer, (ulong)toCopy);

            Memory.Copy(buffer.Pointer, pagePointer, toCopy);
            DecryptPage(pager, (PageHeader*)buffer.Pointer);
            ulong hashFromPager = Hashing.XXHash64.Calculate(buffer.Pointer, (ulong)toCopy);
            
            if(currentHash != hashFromPager)
                throw new InvalidOperationException($"The hash of {pageNumber} was modified, but it was *not* changed in this transaction!");

        }
        private static bool CanReturnBuffer(EncryptionBuffer buffer)
        {
            // Pages that are marked with OriginalSize = 0 were separated from a larger allocation, we cannot free them directly.
            // The first page of the section will be returned and when it will be freed, all the other parts will be freed as well.
            return !buffer.PartOfLargerAllocation;
        }
    }
}
