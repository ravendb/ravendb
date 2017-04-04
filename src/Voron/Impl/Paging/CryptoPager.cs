using System;
using System.Text;
using Sparrow;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Global;

namespace Voron.Impl.Paging
{
    public unsafe class CryptoPager
    {
        private readonly byte[] _masterKey;
        private readonly byte[] _context;

        public CryptoPager()
        {
            _masterKey = Sodium.GenerateMasterKey();
            _context = Sodium.Context;
        }

        private static int GetNumberOfPages(PageHeader* header)
        {
            if ((header->Flags & PageFlags.Overflow) != PageFlags.Overflow)
                return 1;

            var overflowSize = header->OverflowSize + Constants.Tree.PageHeaderSize;
            return checked((overflowSize / Constants.Storage.PageSize) + (overflowSize % Constants.Storage.PageSize == 0 ? 0 : 1));
        }

        private void EncryptPage(ByteStringContext allocator, PageHeader* page, byte* destination)
        {
            var num = page->PageNumber;

            ByteString subKey;
            using (allocator.Allocate(32, out subKey))
            {
                fixed (byte* ctx = _context)
                fixed (byte* mk = _masterKey)
                {
                    if (Sodium.crypto_kdf_derive_from_key(subKey.Ptr, subKey.Length, num, ctx, mk) != 0)
                        throw new InvalidOperationException("Unable to generate derived key");

                    ulong macLen = 16;
                    var dataSize = (ulong)GetNumberOfPages(page) * Constants.Storage.PageSize;

                    var npub = (byte*)(page - macLen - sizeof(long));
                    if (*(long*)npub == 0)
                        Sodium.randombytes_buf(npub, sizeof(long));
                    else
                        (*(long*)npub)++;

                    var rc = Sodium.crypto_aead_chacha20poly1305_encrypt_detached(
                        destination + PageHeader.SizeOf,
                        (byte*)(page + PageHeader.SizeOf - macLen),
                        &macLen,
                        (byte*)(page + PageHeader.SizeOf),
                        dataSize,
                        (byte*)page,
                        PageHeader.SizeOf - macLen - sizeof(long),
                        null,
                        npub,
                        subKey.Ptr
                    );
                    if (rc != 0)
                        throw new InvalidOperationException("Unable to decrypt page " + num);
                }
            }
        }

        private void DecryptPage(ByteStringContext allocator, PageHeader* page, byte* destination)
        {
            var num = page->PageNumber;

            ByteString subKey;
            using (allocator.Allocate(32, out subKey))
            {
                fixed (byte* ctx = _context)
                fixed (byte* mk = _masterKey)
                {
                    if (Sodium.crypto_kdf_derive_from_key(subKey.Ptr, subKey.Length, num, ctx, mk) != 0)
                        throw new InvalidOperationException("Unable to generate derived key");

                    const ulong macLen = 16;
                    var dataSize = (ulong)GetNumberOfPages(page)*Constants.Storage.PageSize;
                    var rc = Sodium.crypto_aead_chacha20poly1305_decrypt_detached(
                        destination + PageHeader.SizeOf,
                        null,
                        (byte*)(page + PageHeader.SizeOf),
                        dataSize - PageHeader.SizeOf,
                        (byte*)(page + PageHeader.SizeOf - macLen),
                        (byte*)page,
                        PageHeader.SizeOf - macLen - sizeof(long),
                        (byte*)(page - macLen - sizeof(long)),
                        subKey.Ptr
                    );
                    if (rc != 0)
                        throw new InvalidOperationException("Unable to decrypt page " + num);
                }
            }
        }
    }
}