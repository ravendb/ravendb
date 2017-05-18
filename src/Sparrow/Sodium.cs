using System;
using System.Text;

namespace Sparrow
{
    public static unsafe class Sodium
    {
        private static int crypto_kdf_keybytes()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                    ? Platform.Posix.PosixSodium.crypto_kdf_keybytes()
                    : Platform.Win32.WinSodium.crypto_kdf_keybytes());
        }

        public static int crypto_kx_keypair(byte* pk, byte* sk)
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_kx_keypair(pk, sk)
                : Platform.Win32.WinSodium.crypto_kx_keypair(pk, sk);

        }

        public static void randombytes_buf(byte* buffer, UIntPtr size)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                Platform.Posix.PosixSodium.randombytes_buf(buffer, size);
            else
                Platform.Win32.WinSodium.randombytes_buf(buffer, size);
        }

        private static void crypto_kdf_keygen(
            byte* masterkey)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
            {
                Platform.Posix.PosixSodium.crypto_kdf_keygen(masterkey);
                return;
            }
            Platform.Win32.WinSodium.crypto_kdf_keygen(masterkey);
        }

        public static int crypto_kdf_derive_from_key(
            byte* subkey,
            UIntPtr subkeylen,
            ulong subkeyid,
            byte* ctx,
            byte* key)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
            return Platform.Win32.WinSodium.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
        }

        public static int crypto_stream_xchacha20_xor_ic(
            byte* c,
            byte* m,
            ulong mlen,
            byte* n,
            ulong ic,
            byte* k)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
            return Platform.Win32.WinSodium.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
        }

        public static int crypto_aead_chacha20poly1305_encrypt_detached(
            byte* c,
            byte* mac,
            // ReSharper disable once InconsistentNaming
            ulong* maclen_p,
            byte* m,
            ulong mlen,
            byte* ad,
            ulong adlen,
            byte* nsec,
            byte* npub,
            byte* k)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_aead_chacha20poly1305_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
            return Platform.Win32.WinSodium.crypto_aead_chacha20poly1305_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
        }

        public static int crypto_aead_chacha20poly1305_encrypt(
            byte* c,
            ulong* clen,
            byte* m,
            ulong mlen,
            byte* ad,
            ulong adlen,
            byte* nsec,
            byte* npub,
            byte* k)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_aead_chacha20poly1305_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
            return Platform.Win32.WinSodium.crypto_aead_chacha20poly1305_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
        }


        public static int crypto_aead_chacha20poly1305_decrypt_detached(
            byte* m,
            byte* nsec,
            byte* c,
            ulong clen,
            byte* mac,
            byte* ad,
            ulong adlen,
            byte* npub,
            byte* k)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_aead_chacha20poly1305_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);
            return Platform.Win32.WinSodium.crypto_aead_chacha20poly1305_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);
        }

        public static int crypto_aead_chacha20poly1305_decrypt(
            byte* m,
            ulong* mlen,
            byte* nsec,
            byte* c,
            ulong clen,
            byte* ad,
            ulong adlen,
            byte* npub,
            byte* k)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_aead_chacha20poly1305_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
            return Platform.Win32.WinSodium.crypto_aead_chacha20poly1305_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
        }

        public static int crypto_box_seal(byte* c, byte* m, ulong mlen, byte* pk)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_box_seal(c, m, mlen, pk);
            return Platform.Win32.WinSodium.crypto_box_seal(c, m, mlen, pk);
        }

        public static int crypto_box_seal_open(byte* m, byte* c,
            ulong clen, byte* pk, byte* sk)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_box_seal_open(m, c, clen, pk, sk);
            return Platform.Win32.WinSodium.crypto_box_seal_open(m, c, clen, pk, sk);
        }

        public static int crypto_stream_xchacha20_keybytes()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_stream_xchacha20_keybytes()
                : Platform.Win32.WinSodium.crypto_stream_xchacha20_keybytes());
        }

        public static int crypto_stream_xchacha20_noncebytes()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_stream_xchacha20_noncebytes()
                : Platform.Win32.WinSodium.crypto_stream_xchacha20_noncebytes());
        }
        
        public static int crypto_box_sealbytes()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_box_sealbytes()
                : Platform.Win32.WinSodium.crypto_box_sealbytes());
        }

        public static int crypto_box_secretkeybytes()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_box_secretkeybytes()
                : Platform.Win32.WinSodium.crypto_box_secretkeybytes());
        }

        public static int crypto_kx_publickeybytes()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_kx_publickeybytes()
                : Platform.Win32.WinSodium.crypto_kx_publickeybytes());
        }

        public static int crypto_kx_secretkeybytes()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_kx_secretkeybytes()
                : Platform.Win32.WinSodium.crypto_kx_secretkeybytes());
        }

        public static int crypto_box_publickeybytes()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_box_publickeybytes()
                : Platform.Win32.WinSodium.crypto_box_publickeybytes());
        }

        public static int crypto_generichash_bytes_max()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_generichash_bytes_max()
                : Platform.Win32.WinSodium.crypto_generichash_bytes_max());
        }

        public static byte[] GenerateMasterKey()
        {
            var masterKey = new byte[crypto_kdf_keybytes()];
            fixed (byte* mk = masterKey)
            {
                crypto_kdf_keygen(mk);
                return masterKey;
            }
        }

        public static readonly byte[] Context = Encoding.UTF8.GetBytes("Raven DB");
        
        public static byte[] GenerateRandomBuffer(int numberOfBits)
        {
            var buffer = new byte[numberOfBits / 8];
            fixed (byte* p = buffer)
            {
                randombytes_buf(p, (UIntPtr)buffer.Length);
            }
            return buffer;
        }

        public static void crypto_box_keypair(byte* pk, byte* sk)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
            {
                Platform.Posix.PosixSodium.crypto_box_keypair(pk, sk);
                return;
            }
            Platform.Win32.WinSodium.crypto_box_keypair(pk, sk);
        }

        public static int crypto_generichash(byte* @out, UIntPtr outlen, byte* @in,
            ulong inlen, byte* key, UIntPtr keylen)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
            return Platform.Win32.WinSodium.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
        }

        public static int sodium_memcmp(byte* b, byte* vh, UIntPtr verifiedHashLength)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.sodium_memcmp(b, vh, verifiedHashLength);
            return Platform.Win32.WinSodium.sodium_memcmp(b, vh, verifiedHashLength);
        }

        public static int crypto_box_macbytes()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_box_macbytes()
                : Platform.Win32.WinSodium.crypto_box_macbytes());
        }

        public static int crypto_box_noncebytes()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_box_noncebytes()
                : Platform.Win32.WinSodium.crypto_box_noncebytes());
        }

        public static int crypto_aead_chacha20poly1305_ABYTES()
        {
            return (int)(Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_aead_chacha20poly1305_abytes()
                : Platform.Win32.WinSodium.crypto_aead_chacha20poly1305_abytes());
        }

        public static int crypto_kx_client_session_keys(
            byte* rx,
            byte* tx,
            byte* client_pk,
            byte* client_sk,
            byte* server_pk)
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk)
                : Platform.Win32.WinSodium.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk);
        }


        public static int crypto_kx_server_session_keys(
            byte* rx,
            byte* tx,
            byte* server_pk,
            byte* server_sk,
            byte* client_pk)
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk)
                : Platform.Win32.WinSodium.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk);
        }

        public static int crypto_box_easy(
            byte* c,
            byte* m,
            ulong mlen,
            byte* n,
            byte* pk,
            byte* sk)
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_box_easy(c, m, mlen, n, pk, sk)
                : Platform.Win32.WinSodium.crypto_box_easy(c, m, mlen, n, pk, sk);

        }

        public static int crypto_box_open_easy(
            byte* m,
            byte* c,
            ulong clen,
            byte* n,
            byte* pk,
            byte* sk)
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_box_open_easy(m, c, clen, n, pk, sk)
                : Platform.Win32.WinSodium.crypto_box_open_easy(m, c, clen, n, pk, sk);
        }

        public static void ZeroMemory(byte* ptr, long size)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                Platform.Posix.PosixSodium.sodium_memzero(ptr, (UIntPtr)size);
            else
                Platform.Win32.WinSodium.sodium_memzero(ptr, (UIntPtr)size);
        }
    }
}
