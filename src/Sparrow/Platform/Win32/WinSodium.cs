using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Sparrow.Platform.Win32
{
    public static unsafe class WinSodium
    {
        private static readonly bool _is32bits;

        static WinSodium()
        {
            if (PlatformDetails.RunningOnPosix)
            {
                return;
            }

            _is32bits = PlatformDetails.Is32Bits;
            if (_is32bits)
            {
                X86.Initialize();
            }
            else
            {
                X64.Initialize();
            }
        }

        // ReSharper disable once InconsistentNaming
        public static UIntPtr crypto_generichash_bytes()
        {
            return _is32bits ? 
                X86.crypto_generichash_bytes() : 
                X64.crypto_generichash_bytes();
        }

        public static UIntPtr crypto_generichash_statebytes()
        {
            return _is32bits ?
                X86.crypto_generichash_statebytes() :
                X64.crypto_generichash_statebytes();
        }

        public static UIntPtr crypto_generichash_keybytes()
        {
            return _is32bits ?
                X86.crypto_generichash_keybytes() :
                X64.crypto_generichash_keybytes();
        }

        public static int crypto_generichash_init(void* /* crypto_generichash_state */ state,
            byte* key,
            UIntPtr keylen,
            UIntPtr outlen)
        {
            return _is32bits ?
                X86.crypto_generichash_init(state, key, keylen, outlen) :
                X64.crypto_generichash_init(state, key, keylen, outlen);
        }

        public static int crypto_generichash_update(
            void* /* crypto_generichash_state */ state,
            byte* @in,
            ulong inlen)
        {
            return _is32bits ?
                X86.crypto_generichash_update(state, @in, inlen) :
                X64.crypto_generichash_update(state, @in, inlen);
        }

        public static int crypto_generichash_final(
            void* /* crypto_generichash_state */ state,
            byte* @out,
            UIntPtr outlen)
        {
            return _is32bits ?
                X86.crypto_generichash_final(state, @out, outlen) :
                X64.crypto_generichash_final(state, @out, outlen);
        }

        public static int crypto_kx_keypair(
            byte* pk,
            byte* sk)
        {
            return _is32bits ?
                X86.crypto_kx_keypair(pk, sk) :
                X64.crypto_kx_keypair(pk, sk);
        }

        public static UIntPtr crypto_kdf_keybytes()
        {
            return _is32bits ?
                X86.crypto_kdf_keybytes() :
                X64.crypto_kdf_keybytes();
        }

        public static UIntPtr crypto_stream_xchacha20_keybytes()
        {
            return _is32bits ?
                X86.crypto_stream_xchacha20_keybytes() :
                X64.crypto_stream_xchacha20_keybytes();
        }

        public static UIntPtr crypto_stream_xchacha20_noncebytes()
        {
            return _is32bits ?
                X86.crypto_stream_xchacha20_noncebytes() :
                X64.crypto_stream_xchacha20_noncebytes();
        }

        public static void randombytes_buf(
            byte* buffer,
            UIntPtr size)
        {
            if (_is32bits)
                X86.randombytes_buf(buffer, size);
            else
                X64.randombytes_buf(buffer, size);
        }

        public static void crypto_kdf_keygen(
            byte* masterkey)
        {
            if (_is32bits)
                X86.crypto_kdf_keygen(masterkey);
            else
                X64.crypto_kdf_keygen(masterkey);
        }

        public static int crypto_kdf_derive_from_key(
            byte* subkey,
            UIntPtr subkeylen,
            ulong subkeyid,
            byte* ctx,
            byte* key)
        {
            return _is32bits
                ? X86.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key)
                : X64.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
        }

        public static int crypto_stream_xchacha20_xor_ic(
            byte* c,
            byte* m,
            ulong mlen,
            byte* n,
            ulong ic,
            byte* k)
        {
            return _is32bits ?
                X86.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k) :
                X64.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
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
            return _is32bits ?
                X86.crypto_aead_chacha20poly1305_encrypt_detached(
                    c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k) :
                X64.crypto_aead_chacha20poly1305_encrypt_detached(
                    c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k) ;
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
            return _is32bits
                ? X86.crypto_aead_chacha20poly1305_decrypt_detached(
                    m, nsec, c, clen, mac, ad, adlen, npub, k)
                : X64.crypto_aead_chacha20poly1305_decrypt_detached(
                    m, nsec, c, clen, mac, ad, adlen, npub, k);

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
            return _is32bits
                ? X86.crypto_aead_chacha20poly1305_encrypt(
                    c, clen, m, mlen, ad, adlen, nsec, npub, k)
                : X64.crypto_aead_chacha20poly1305_encrypt(
                    c, clen, m, mlen, ad, adlen, nsec, npub, k);
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
            return _is32bits
                ? X86.crypto_aead_chacha20poly1305_decrypt(
                    m, mlen, nsec, c, clen, ad, adlen, npub, k)
                : X64.crypto_aead_chacha20poly1305_decrypt(
                    m, mlen, nsec, c, clen, ad, adlen, npub, k);
        }

        public static int crypto_box_seal(
            byte* b,
            byte* b1,
            ulong mlen,
            byte* pk)
        {
            return _is32bits
                ? X86.crypto_box_seal(b, b1, mlen, pk)
                : X64.crypto_box_seal(b, b1, mlen, pk);
        }

        public static int crypto_box_seal_open(
            byte* m,
            byte* c,
            ulong clen,
            byte* pk,
            byte* sk)
        {
            return _is32bits
                ? X86.crypto_box_seal_open(
                    m, c, clen, pk, sk)
                : X64.crypto_box_seal_open(
                    m, c, clen, pk, sk);
        }

        public static UIntPtr crypto_box_sealbytes()
        {
            return _is32bits
                ? X86.crypto_box_sealbytes()
                : X64.crypto_box_sealbytes();
        }

        public static int crypto_generichash(
            byte* @out,
            UIntPtr outlen,
            byte* @in,
            ulong inlen,
            byte* key,
            UIntPtr keylen)
        {
            return _is32bits
                ? X86.crypto_generichash(
                    @out, outlen, @in, inlen, key, keylen)
                : X64.crypto_generichash(
                    @out, outlen, @in, inlen, key, keylen);
        }

        public static UIntPtr crypto_generichash_bytes_max()
        {
            return _is32bits
                ? X86.crypto_generichash_bytes_max()
                : X64.crypto_generichash_bytes_max();
        }

        public static int crypto_box_keypair(
            byte* pk,
            byte* sk)
        {
            return _is32bits ?
                X86.crypto_box_keypair(pk, sk) :
                X64.crypto_box_keypair(pk, sk);
        }

        public static int sodium_memcmp(
            byte* b1,
            byte* b2,
            UIntPtr len)
        {
            return _is32bits ?
                X86.sodium_memcmp(b1, b2, len) :
                X64.sodium_memcmp(b1, b2, len);
        }

        public static UIntPtr crypto_box_secretkeybytes()
        {
            return _is32bits ?
                X86.crypto_box_secretkeybytes() :
                X64.crypto_box_secretkeybytes();
        }

        public static UIntPtr crypto_box_publickeybytes()
        {
            return _is32bits ?
                X86.crypto_box_publickeybytes() :
                X64.crypto_box_publickeybytes();
        }

        public static UIntPtr crypto_kx_publickeybytes()
        {
            return _is32bits ?
                X86.crypto_kx_publickeybytes() :
                X64.crypto_kx_publickeybytes();
        }

        public static UIntPtr crypto_kx_secretkeybytes()
        {
            return _is32bits ? 
                X86.crypto_kx_secretkeybytes() : 
                X64.crypto_kx_secretkeybytes();
        }

        public static UIntPtr crypto_box_macbytes()
        {
            return _is32bits ? 
                X86.crypto_box_macbytes() : 
                X64.crypto_box_macbytes();
        }

        public static UIntPtr crypto_box_noncebytes()
        {
            return _is32bits ?
                X86.crypto_box_noncebytes() :
                X64.crypto_box_noncebytes();
        }

        public static UIntPtr crypto_aead_chacha20poly1305_abytes()
        {
            return _is32bits
                ? X86.crypto_aead_chacha20poly1305_abytes()
                : X64.crypto_aead_chacha20poly1305_abytes();
        }

        public static int crypto_kx_client_session_keys(
            byte* rx,
            byte* tx,
            byte* client_pk,
            byte* client_sk,
            byte* server_pk)
        {
            return _is32bits
                ? X86.crypto_kx_client_session_keys(
                    rx, tx, client_pk, client_sk, server_pk)
                : X64.crypto_kx_client_session_keys(
                    rx, tx, client_pk, client_sk, server_pk);
        }

        public static int crypto_kx_server_session_keys(
            byte* rx,
            byte* tx,
            byte* server_pk,
            byte* server_sk,
            byte* client_pk)
        {
            return _is32bits
                ? X86.crypto_kx_server_session_keys(
                    rx, tx, server_pk, server_sk, client_pk)
                : X64.crypto_kx_server_session_keys(
                    rx, tx, server_pk, server_sk, client_pk);
        }

        public static int crypto_box_easy(
            byte* c,
            byte* m,
            ulong mlen,
            byte* n,
            byte* pk,
            byte* sk)
        {
            return _is32bits
                ? X86.crypto_box_easy(c, m, mlen, n, pk, sk)
                : X64.crypto_box_easy(c, m, mlen, n, pk, sk);
        }

        public static int crypto_box_open_easy(
            byte* m,
            byte* c,
            ulong clen,
            byte* n,
            byte* pk,
            byte* sk)
        {
            return _is32bits
                ? X86.crypto_box_open_easy(m, c, clen, n, pk, sk)
                : X64.crypto_box_open_easy(m, c, clen, n, pk, sk);
        }

        public static int crypto_sign_detached(
            byte* sig,
            ulong* siglen,
            byte* m,
            ulong mlen,
            byte* sk)
        {
            return _is32bits
                ? X86.crypto_sign_detached(sig, siglen, m, mlen, sk)
                : X64.crypto_sign_detached(sig, siglen, m, mlen, sk);
        }

        public static int crypto_sign_verify_detached(
            byte* sig,
            byte* m,
            ulong mlen,
            byte* pk)
        {
            return _is32bits
                ? X86.crypto_sign_verify_detached(sig, m, mlen, pk)
                : X64.crypto_sign_verify_detached(sig, m, mlen, pk);
        }

        public static UIntPtr crypto_sign_publickeybytes()
        {
            return _is32bits
                ? X86.crypto_sign_publickeybytes()
                : X64.crypto_sign_publickeybytes();
        }

        public static UIntPtr crypto_sign_secretkeybytes()
        {
            return _is32bits
                ? X86.crypto_sign_secretkeybytes()
                : X64.crypto_sign_secretkeybytes();
        }

        public static UIntPtr crypto_sign_bytes()
        {
            return _is32bits
                ? X86.crypto_sign_bytes()
                : X64.crypto_sign_bytes();
        }

        public static int crypto_sign_keypair(
            byte* pk,
            byte* sk)
        {
            return _is32bits
                ? X86.crypto_sign_keypair(pk, sk)
                : X64.crypto_sign_keypair(pk, sk);
        }

        public static void sodium_memzero(
            byte* pnt,
            UIntPtr len)
        {
            if (_is32bits)
                X86.sodium_memzero(pnt, len);
            else
                X64.sodium_memzero(pnt, len);
        }

        private static class X86
        {
            private const string LIB_SODIUM = "libsodium.x86.dll";

            public static void Initialize()
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
                byte* @out,
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
            public static extern int crypto_sign_detached(
                byte* sig,
                ulong* siglen,
                byte* m,
                ulong mlen,
                byte* sk);

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_verify_detached(
                byte* sig,
                byte* m,
                ulong mlen,
                byte* pk);

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_sign_publickeybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_sign_secretkeybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_sign_bytes();

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_keypair(
                byte* pk,
                byte* sk);

            [DllImport(LIB_SODIUM)]
            public static extern void sodium_memzero(
                byte* pnt,
                UIntPtr len);

        }

        private static class X64
        {
            private const string LIB_SODIUM = "libsodium.x64.dll";

            public static void Initialize()
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
                byte* @out,
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
            public static extern int crypto_sign_detached(
                byte* sig,
                ulong* siglen,
                byte* m,
                ulong mlen,
                byte* sk);

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_verify_detached(
                byte* sig,
                byte* m,
                ulong mlen,
                byte* pk);

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_sign_publickeybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_sign_secretkeybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_sign_bytes();

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_keypair(
                byte* pk,
                byte* sk);

            [DllImport(LIB_SODIUM)]
            public static extern void sodium_memzero(
                byte* pnt,
                UIntPtr len);

        }

    }
}