using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Sparrow.Platform.Win32
{
    public static unsafe class WinSodium
    {
        // ReSharper disable once InconsistentNaming
        private const string LIB_SODIUM = "libsodium.dll";

        static WinSodium()
        {
            try
            {
                var rc = sodium_init();
                if (rc != 0)
                    throw new InvalidOperationException("Unable to initialize sodium, error code: " + rc);
            }
            catch (DllNotFoundException dllNotFoundEx)
            {
                // make sure the lib file is not there (this exception might pop when incorrect libsodium lib is does exists)
                if (File.Exists(LIB_SODIUM))
                {
                    throw new IncorrectDllException(
                            $"{LIB_SODIUM} probably contains the wrong version or not usable on the current platform. Make sure that this machine has the appropriate C runtime for {LIB_SODIUM}.",
                            dllNotFoundEx);
                }
            }
        }

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_generichash_bytes();

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_generichash_statebytes();

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_generichash_keybytes();

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_generichash_init(void* /* crypto_generichash_state */ state,
            byte* key,
            UIntPtr keylen,
            UIntPtr outlen);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_generichash_update(
            void* /* crypto_generichash_state */ state,
            byte* @in,
            ulong inlen);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_generichash_final(
            void* /* crypto_generichash_state */ state,
            byte*@out,
            UIntPtr outlen);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_kx_keypair(
            byte* pk,
            byte* sk);

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_kdf_keybytes();

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_stream_xchacha20_keybytes();

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_stream_xchacha20_noncebytes();

        [DllImport(LIB_SODIUM)]
        public static extern int sodium_init();

        [DllImport(LIB_SODIUM)]
        public static extern void randombytes_buf(
            byte* buffer,
            UIntPtr size);

        [DllImport(LIB_SODIUM)]
        public static extern void crypto_kdf_keygen(
            byte* masterkey);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_kdf_derive_from_key(
            byte* subkey,
            UIntPtr subkeylen,
            ulong subkeyid,
            byte* ctx,
            byte* key);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_stream_xchacha20_xor_ic(
            byte* c,
            byte* m,
            ulong mlen,
            byte* n,
            ulong ic,
            byte* k);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_aead_chacha20poly1305_encrypt_detached(
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
            byte* k);


        [DllImport(LIB_SODIUM)]
        public static extern int crypto_aead_chacha20poly1305_decrypt_detached(
            byte* m,
            byte* nsec,
            byte* c,
            ulong clen,
            byte* mac,
            byte* ad,
            ulong adlen,
            byte* npub,
            byte* k);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_aead_chacha20poly1305_encrypt(
            byte* c,
            ulong* clen,
            byte* m,
            ulong mlen,
            byte* ad,
            ulong adlen,
            byte* nsec,
            byte* npub,
            byte* k);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_aead_chacha20poly1305_decrypt(
            byte* m,
            ulong* mlen,
            byte* nsec,
            byte* c,
            ulong clen,
            byte* ad,
            ulong adlen,
            byte* npub,
            byte* k);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_box_seal(
            byte* b,
            byte* b1,
            ulong mlen,
            byte* pk);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_box_seal_open(
            byte* m,
            byte* c,
            ulong clen,
            byte* pk,
            byte* sk);

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_box_sealbytes();

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_generichash(
            byte* @out,
            UIntPtr outlen,
            byte* @in,
            ulong inlen,
            byte* key,
            UIntPtr keylen);

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_generichash_bytes_max();

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_box_keypair(
            byte* pk,
            byte* sk);

        [DllImport(LIB_SODIUM)]
        public static extern int sodium_memcmp(
            byte* b1,
            byte* b2,
            UIntPtr len);

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_box_secretkeybytes();

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_box_publickeybytes();

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_kx_publickeybytes();

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_kx_secretkeybytes();

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_box_macbytes();

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_box_noncebytes();

        [DllImport(LIB_SODIUM)]
        public static extern UIntPtr crypto_aead_chacha20poly1305_abytes();

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_kx_client_session_keys(
            byte* rx,
            byte* tx,
            byte* client_pk,
            byte* client_sk,
            byte* server_pk);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_kx_server_session_keys(
            byte* rx,
            byte* tx,
            byte* server_pk,
            byte* server_sk,
            byte* client_pk);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_box_easy(
            byte* c,
            byte* m,
            ulong mlen,
            byte* n,
            byte* pk,
            byte* sk);

        [DllImport(LIB_SODIUM)]
        public static extern int crypto_box_open_easy(
            byte* m,
            byte* c,
            ulong clen,
            byte* n,
            byte* pk,
            byte* sk);

        [DllImport(LIB_SODIUM)]
        public static extern void sodium_memzero(
            byte* pnt,
            UIntPtr len);
    }
}