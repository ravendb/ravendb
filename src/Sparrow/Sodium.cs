using System;
using System.Text;

namespace Sparrow
{
    public static unsafe class Sodium
    {
        private static int crypto_kdf_keybytes()
        {
            return
                Platform.PlatformDetails.RunningOnPosix
                    ? Platform.Posix.PosixSodium.crypto_kdf_keybytes()
                    : Platform.Win32.WinSodium.crypto_kdf_keybytes();
        }

        public static int crypto_kx_keypair(byte* pk, byte* sk)
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_kx_keypair(pk, sk)
                : Platform.Win32.WinSodium.crypto_kx_keypair(pk, sk);

        }

        public static void randombytes_buf(byte* buffer, int size)
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
            int subkeylen,
            long subkeyid,
            byte* ctx,
            byte* key)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
            return Platform.Win32.WinSodium.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
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

        public static int crypto_box_sealbytes()
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_box_sealbytes()
                : Platform.Win32.WinSodium.crypto_box_sealbytes();
        }

        public static int crypto_box_secretkeybytes()
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_box_secretkeybytes()
                : Platform.Win32.WinSodium.crypto_box_secretkeybytes();
        }

        public static int crypto_kx_publickeybytes()
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_kx_publickeybytes()
                : Platform.Win32.WinSodium.crypto_kx_publickeybytes();
        }

        public static int crypto_kx_secretkeybytes()
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_kx_secretkeybytes()
                : Platform.Win32.WinSodium.crypto_kx_secretkeybytes();
        }

        public static int crypto_box_publickeybytes()
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_box_publickeybytes()
                : Platform.Win32.WinSodium.crypto_box_publickeybytes();
        }

        public static int crypto_generichash_bytes_max()
        {
            return Platform.PlatformDetails.RunningOnPosix
                ? Platform.Posix.PosixSodium.crypto_generichash_bytes_max()
                : Platform.Win32.WinSodium.crypto_generichash_bytes_max();
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

        public static byte[] DeriveKey(byte[] masterKey, long num)
        {
            var subKey = new byte[256 / 8];

            fixed (byte* key = masterKey)
            fixed (byte* sk = subKey)
            fixed (byte* ctx = Context)
            {
                var rc = crypto_kdf_derive_from_key(sk,
                    subKey.Length,
                    num,
                    ctx,
                    key
                );

                if (rc != 0)
                    throw new InvalidOperationException("Could not derive key from " + num + " because " + rc);

                return subKey;
            }
        }

        public static byte[] GenerateKey()
        {
            return GenerateRandomBuffer(256);
        }

        public static byte[] GenerateNonce()
        {
            return GenerateRandomBuffer(64);
        }

        public static byte[] GenerateRandomBuffer(int numberOfBits)
        {
            var buffer = new byte[numberOfBits / 8];
            fixed (byte* p = buffer)
            {
                randombytes_buf(p, buffer.Length);
            }
            return buffer;
        }

        public static byte[] AeadChacha20Poly1305Encrypt(byte[] key, byte[] nonce, byte[] message, byte[] additionalData, byte[] mac)
        {
            ulong macLength = 16;
            var cipher = new byte[(ulong)message.Length];

            fixed (byte* c = cipher)
            fixed (byte* k = key)
            fixed (byte* n = nonce)
            fixed (byte* m = message)
            fixed (byte* mc = mac)
            fixed (byte* ad = additionalData)
            {
                var rc = crypto_aead_chacha20poly1305_encrypt_detached(
                    c,
                    mc,
                    &macLength,
                    m,
                    (ulong)message.Length,
                    ad,
                    (ulong)additionalData.Length,
                    null,
                    n,
                    k
                );
                if (rc != 0)
                    throw new InvalidOperationException("Failed to call crypto_aead_xchacha20poly1305_ietf_encrypt, rc = " + rc);

                return cipher;
            }
        }

        public static byte[] AeadChacha20Poly1305Decrypt(byte[] key, byte[] nonce, byte[] cipher, byte[] additionalData, byte[] mac)
        {
            var message = new byte[cipher.Length];

            fixed (byte* c = cipher)
            fixed (byte* k = key)
            fixed (byte* n = nonce)
            fixed (byte* m = message)
            fixed (byte* mc = mac)
            fixed (byte* ad = additionalData)
            {
                var rc = crypto_aead_chacha20poly1305_decrypt_detached(
                    m,
                    null,
                    c,
                    (ulong)cipher.Length,
                    mc,
                    ad,
                    (ulong)additionalData.Length,
                    n,
                    k
                );
                if (rc != 0)
                    throw new InvalidOperationException("Failed to call crypto_aead_xchacha20poly1305_ietf_decrypt, rc = " + rc);

                return message;
            }

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

        public static int crypto_generichash(byte* @out, IntPtr outlen, byte* @in,
            ulong inlen, byte* key, IntPtr keylen)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
            return Platform.Win32.WinSodium.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
        }

        public static int sodium_memcmp(byte* b, byte* vh, IntPtr verifiedHashLength)
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
            long mlen,
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
            long clen,
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
                Platform.Posix.PosixSodium.sodium_memzero(ptr, (IntPtr)size);
            else
                Platform.Win32.WinSodium.sodium_memzero(ptr, (IntPtr)size);
        }
    }
}
