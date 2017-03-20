using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow
{
    public static unsafe class Sodium
    {
        static Sodium()
        {
            var rc = sodium_init();
            if (rc != 0)
                throw new InvalidOperationException("Unable to initialize sodium, error code: " + rc);
        }


        private static int crypto_kdf_keybytes()
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.crypto_kdf_keybytes();
            return Platform.Win32.WinSodium.crypto_kdf_keybytes();
        }

        private static int sodium_init()
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.sodium_init();
            return Platform.Win32.WinSodium.sodium_init();
        }

        public static int randombytes_buf(
            byte* buffer,
            int size)
        {
            if (Platform.PlatformDetails.RunningOnPosix)
                return Platform.Posix.PosixSodium.randombytes_buf(buffer, size);
            return Platform.Win32.WinSodium.randombytes_buf(buffer, size);
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

        private static byte[] GenerateRandomBuffer(int numberOfBits)
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
    }
}
