using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Sparrow.Platform.Posix
{
    public static unsafe class PosixSodium
    {
        private static readonly bool _is32bits;
        private static readonly bool _isArm;
        private static bool _isMac64;

        static PosixSodium()
        {
            if (PlatformDetails.RunningOnPosix == false)
            {
                return;
            }

            _is32bits = PlatformDetails.Is32Bits;
            _isArm = RuntimeInformation.OSArchitecture == Architecture.Arm;
            _isMac64 = PlatformDetails.RunningOnMacOsx;

            if (_is32bits)
            {
                if (_isMac64)
                {
                    throw new PlatformNotSupportedException("RavenDB cannot be executed on 32-bit MacOS. Please use Mac OSX El Capitan or higher");
                }
                if (_isArm)
                {
                    Arm.Initialize();
                }
                else
                {
                    X86.Initialize();
                }
            }
            else
            {
                if (PlatformDetails.RunningOnMacOsx)
                    MacOsxX64.Initialize();
                else
                    X64.Initialize();
            }

            // test for existance of libsodium:
            try
            {
                crypto_sign_publickeybytes();
            }
            catch (Exception e)
            {
                if (_isMac64)
                    throw new Exception("Make sure libsodium is installed on your Mac OSX. Use `brew install libsodium`", e);

                if (PlatformDetails.RunningOnPosix)
                    throw new Exception("Make sure libsodium is installed on your Linux OS. (install package `libsodium` or `libsodium-18`", e);

                throw new Exception("Make sure libsodium is installed on your Windows OS.", e);
            }
        }

        // ReSharper disable once InconsistentNaming
        public static UIntPtr crypto_generichash_bytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_generichash_bytes() : X86.crypto_generichash_bytes()) : _isMac64 ? MacOsxX64.crypto_generichash_bytes() : X64.crypto_generichash_bytes();
        }

        public static UIntPtr crypto_sign_statebytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_sign_statebytes() : X86.crypto_sign_statebytes()) : _isMac64 ? MacOsxX64.crypto_sign_statebytes() : X64.crypto_sign_statebytes();
        }

        public static UIntPtr crypto_generichash_statebytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_generichash_statebytes() : X86.crypto_generichash_statebytes()) : _isMac64 ? MacOsxX64.crypto_generichash_statebytes() : X64.crypto_generichash_statebytes();
        }

        public static UIntPtr crypto_generichash_keybytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_generichash_keybytes() : X86.crypto_generichash_keybytes()) : _isMac64 ? MacOsxX64.crypto_generichash_keybytes() : X64.crypto_generichash_keybytes();
        }

        public static int crypto_generichash_init(void* /* crypto_generichash_state */ state,
            byte* key,
            UIntPtr keylen,
            UIntPtr outlen)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_generichash_init(state, key, keylen, outlen) : X86.crypto_generichash_init(state, key, keylen, outlen))
                : _isMac64 ? MacOsxX64.crypto_generichash_init(state, key, keylen, outlen) : X64.crypto_generichash_init(state, key, keylen, outlen);
        }

        public static int crypto_sign_init(void* /* crypto_sign_state  */ state)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_sign_init(state) : X86.crypto_sign_init(state))
                : _isMac64 ? MacOsxX64.crypto_sign_init(state) : X64.crypto_sign_init(state);
        }


        public static int crypto_sign_update(void* /* crypto_generichash_state */ state,
            byte* m,
            ulong mlen)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_sign_update(state, m, mlen) : X86.crypto_sign_update(state, m, mlen))
                : _isMac64 ? MacOsxX64.crypto_sign_update(state, m, mlen) : X64.crypto_sign_update(state, m, mlen);
        }

        public static int crypto_sign_final_create(void* /* crypto_generichash_state */ state,
            byte* sig,
            ulong* siglen_p,
            byte* sk)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_sign_final_create(state, sig, siglen_p, sk) : X86.crypto_sign_final_create(state, sig, siglen_p, sk))
                : _isMac64 ? MacOsxX64.crypto_sign_final_create(state, sig, siglen_p, sk) : X64.crypto_sign_final_create(state, sig, siglen_p, sk);

        }

        public static int crypto_generichash_update(
            void* /* crypto_generichash_state */ state,
            byte* @in,
            ulong inlen)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_generichash_update(state, @in, inlen) : X86.crypto_generichash_update(state, @in, inlen))
                : _isMac64 ? MacOsxX64.crypto_generichash_update(state, @in, inlen) : X64.crypto_generichash_update(state, @in, inlen);
        }

        public static int crypto_generichash_final(
            void* /* crypto_generichash_state */ state,
            byte* @out,
            UIntPtr outlen)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_generichash_final(state, @out, outlen) : X86.crypto_generichash_final(state, @out, outlen))
                : _isMac64 ? MacOsxX64.crypto_generichash_final(state, @out, outlen) : X64.crypto_generichash_final(state, @out, outlen);
        }

        public static int crypto_kx_keypair(
            byte* pk,
            byte* sk)
        {
            return _is32bits ? (_isArm ? Arm.crypto_kx_keypair(pk, sk) : X86.crypto_kx_keypair(pk, sk)) : _isMac64 ? MacOsxX64.crypto_kx_keypair(pk, sk) : X64.crypto_kx_keypair(pk, sk);
        }

        public static UIntPtr crypto_kdf_keybytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_kdf_keybytes() : X86.crypto_kdf_keybytes()) : _isMac64 ? MacOsxX64.crypto_kdf_keybytes() : X64.crypto_kdf_keybytes();
        }

        public static UIntPtr crypto_stream_xchacha20_keybytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_stream_xchacha20_keybytes() : X86.crypto_stream_xchacha20_keybytes()) : _isMac64 ? MacOsxX64.crypto_stream_xchacha20_keybytes() : X64.crypto_stream_xchacha20_keybytes();
        }

        public static UIntPtr crypto_stream_xchacha20_noncebytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_stream_xchacha20_noncebytes() : X86.crypto_stream_xchacha20_noncebytes()) : _isMac64 ? MacOsxX64.crypto_stream_xchacha20_noncebytes() : X64.crypto_stream_xchacha20_noncebytes();
        }

        public static UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_aead_xchacha20poly1305_ietf_keybytes() : X86.crypto_aead_xchacha20poly1305_ietf_keybytes()) : _isMac64 ? MacOsxX64.crypto_aead_xchacha20poly1305_ietf_keybytes() : X64.crypto_aead_xchacha20poly1305_ietf_keybytes();
        }

        public static UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_aead_xchacha20poly1305_ietf_npubbytes() : X86.crypto_aead_xchacha20poly1305_ietf_npubbytes()) : _isMac64 ? MacOsxX64.crypto_aead_xchacha20poly1305_ietf_npubbytes() : X64.crypto_aead_xchacha20poly1305_ietf_npubbytes();
        }

        public static UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_aead_xchacha20poly1305_ietf_abytes() : X86.crypto_aead_xchacha20poly1305_ietf_abytes()) : _isMac64 ? MacOsxX64.crypto_aead_xchacha20poly1305_ietf_abytes() : X64.crypto_aead_xchacha20poly1305_ietf_abytes();
        }

        public static void randombytes_buf(
            byte* buffer,
            UIntPtr size)
        {
            if (_is32bits)
            {
                if (_isArm)
                    Arm.randombytes_buf(buffer, size);
                else
                    X86.randombytes_buf(buffer, size);
            }
            else
            {
                if (_isMac64)
                    MacOsxX64.randombytes_buf(buffer, size);
                else
                    X64.randombytes_buf(buffer, size);
            }
        }


        public static void crypto_kdf_keygen(
            byte* masterkey)
        {
            if (_is32bits)
            {
                if (_isArm)
                    Arm.crypto_kdf_keygen(masterkey);
                else
                    X86.crypto_kdf_keygen(masterkey);
            }
            else
            {
                if (_isMac64)
                    MacOsxX64.crypto_kdf_keygen(masterkey);
                else
                    X64.crypto_kdf_keygen(masterkey);
            }
        }

        public static int crypto_kdf_derive_from_key(
            byte* subkey,
            UIntPtr subkeylen,
            ulong subkeyid,
            byte* ctx,
            byte* key)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key) : X86.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key))
                : _isMac64 ? MacOsxX64.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key) : X64.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
        }

        public static int crypto_stream_xchacha20_xor_ic(
            byte* c,
            byte* m,
            ulong mlen,
            byte* n,
            ulong ic,
            byte* k)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k) : X86.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k))
                : _isMac64 ? MacOsxX64.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k) : X64.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
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
            return _is32bits
                ? (_isArm
                    ? Arm.crypto_aead_chacha20poly1305_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k)
                    : X86.crypto_aead_chacha20poly1305_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k))
                : _isMac64 ? MacOsxX64.crypto_aead_chacha20poly1305_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k) : X64.crypto_aead_chacha20poly1305_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
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
                ? (_isArm
                    ? Arm.crypto_aead_chacha20poly1305_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k)
                    : X86.crypto_aead_chacha20poly1305_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k))
                : _isMac64 ? MacOsxX64.crypto_aead_chacha20poly1305_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k) : X64.crypto_aead_chacha20poly1305_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);

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
                ? (_isArm
                    ? Arm.crypto_aead_chacha20poly1305_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k)
                    : X86.crypto_aead_chacha20poly1305_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k))
                : _isMac64 ? MacOsxX64.crypto_aead_chacha20poly1305_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k) : X64.crypto_aead_chacha20poly1305_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
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
                ? (_isArm
                    ? Arm.crypto_aead_chacha20poly1305_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k)
                    : X86.crypto_aead_chacha20poly1305_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k))
                : _isMac64 ? MacOsxX64.crypto_aead_chacha20poly1305_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k) : X64.crypto_aead_chacha20poly1305_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
        }

        public static int crypto_aead_xchacha20poly1305_ietf_encrypt(
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
                ? (_isArm
                    ? Arm.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k)
                    : X86.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k))
                : _isMac64 ? MacOsxX64.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k) : X64.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
        }

        public static int crypto_aead_xchacha20poly1305_ietf_decrypt(
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
                ? (_isArm
                    ? Arm.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k)
                    : X86.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k))
                : _isMac64 ? MacOsxX64.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k) : X64.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
        }

        public static int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(
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
            return _is32bits
                ? (_isArm
                    ? Arm.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k)
                    : X86.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k))
                : _isMac64 ? MacOsxX64.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k) : X64.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
        }

        public static int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(
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
                ? (_isArm
                    ? Arm.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k)
                    : X86.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k))
                : _isMac64 ? MacOsxX64.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k) : X64.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);

        }

        public static int crypto_box_seal(
            byte* b,
            byte* b1,
            ulong mlen,
            byte* pk)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_box_seal(b, b1, mlen, pk) : X86.crypto_box_seal(b, b1, mlen, pk))
                : _isMac64 ? MacOsxX64.crypto_box_seal(b, b1, mlen, pk) : X64.crypto_box_seal(b, b1, mlen, pk);
        }

        public static int crypto_box_seal_open(
            byte* m,
            byte* c,
            ulong clen,
            byte* pk,
            byte* sk)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_box_seal_open(m, c, clen, pk, sk) : X86.crypto_box_seal_open(m, c, clen, pk, sk))
                : _isMac64 ? MacOsxX64.crypto_box_seal_open(m, c, clen, pk, sk) : X64.crypto_box_seal_open(m, c, clen, pk, sk);
        }

        public static UIntPtr crypto_box_sealbytes()
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_box_sealbytes() : X86.crypto_box_sealbytes())
                : _isMac64 ? MacOsxX64.crypto_box_sealbytes() : X64.crypto_box_sealbytes();
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
                ? (_isArm ? Arm.crypto_generichash(@out, outlen, @in, inlen, key, keylen) : X86.crypto_generichash(@out, outlen, @in, inlen, key, keylen))
                : _isMac64 ? MacOsxX64.crypto_generichash(@out, outlen, @in, inlen, key, keylen) : X64.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
        }

        public static UIntPtr crypto_generichash_bytes_max()
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_generichash_bytes_max() : X86.crypto_generichash_bytes_max())
                : _isMac64 ? MacOsxX64.crypto_generichash_bytes_max() : X64.crypto_generichash_bytes_max();
        }

        public static int crypto_box_keypair(
            byte* pk,
            byte* sk)
        {
            return _is32bits ? (_isArm ? Arm.crypto_box_keypair(pk, sk) : X86.crypto_box_keypair(pk, sk)) : _isMac64 ? MacOsxX64.crypto_box_keypair(pk, sk) : X64.crypto_box_keypair(pk, sk);
        }

        public static int sodium_memcmp(
            byte* b1,
            byte* b2,
            UIntPtr len)
        {
            return _is32bits ? (_isArm ? Arm.sodium_memcmp(b1, b2, len) : X86.sodium_memcmp(b1, b2, len)) : _isMac64 ? MacOsxX64.sodium_memcmp(b1, b2, len) : X64.sodium_memcmp(b1, b2, len);
        }

        public static UIntPtr crypto_box_secretkeybytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_box_secretkeybytes() : X86.crypto_box_secretkeybytes()) : _isMac64 ? MacOsxX64.crypto_box_secretkeybytes() : X64.crypto_box_secretkeybytes();
        }

        public static UIntPtr crypto_box_publickeybytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_box_publickeybytes() : X86.crypto_box_publickeybytes()) : _isMac64 ? MacOsxX64.crypto_box_publickeybytes() : X64.crypto_box_publickeybytes();
        }

        public static UIntPtr crypto_kx_publickeybytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_kx_publickeybytes() : X86.crypto_kx_publickeybytes()) : _isMac64 ? MacOsxX64.crypto_kx_publickeybytes() : X64.crypto_kx_publickeybytes();
        }

        public static UIntPtr crypto_kx_secretkeybytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_kx_secretkeybytes() : X86.crypto_kx_secretkeybytes()) : _isMac64 ? MacOsxX64.crypto_kx_secretkeybytes() : X64.crypto_kx_secretkeybytes();
        }

        public static UIntPtr crypto_box_macbytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_box_macbytes() : X86.crypto_box_macbytes()) : _isMac64 ? MacOsxX64.crypto_box_macbytes() : X64.crypto_box_macbytes();
        }

        public static UIntPtr crypto_box_noncebytes()
        {
            return _is32bits ? (_isArm ? Arm.crypto_box_noncebytes() : X86.crypto_box_noncebytes()) : _isMac64 ? MacOsxX64.crypto_box_noncebytes() : X64.crypto_box_noncebytes();
        }

        public static UIntPtr crypto_aead_chacha20poly1305_abytes()
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_aead_chacha20poly1305_abytes() : X86.crypto_aead_chacha20poly1305_abytes())
                : _isMac64 ? MacOsxX64.crypto_aead_chacha20poly1305_abytes() : X64.crypto_aead_chacha20poly1305_abytes();
        }

        public static int crypto_kx_client_session_keys(
            byte* rx,
            byte* tx,
            byte* client_pk,
            byte* client_sk,
            byte* server_pk)
        {
            return _is32bits
                ? (_isArm
                    ? Arm.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk)
                    : X86.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk))
                : _isMac64 ? MacOsxX64.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk) : X64.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk);
        }

        public static int crypto_kx_server_session_keys(
            byte* rx,
            byte* tx,
            byte* server_pk,
            byte* server_sk,
            byte* client_pk)
        {
            return _is32bits
                ? (_isArm
                    ? Arm.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk)
                    : X86.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk))
                : _isMac64 ? MacOsxX64.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk) : X64.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk);
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
                ? (_isArm ? Arm.crypto_box_easy(c, m, mlen, n, pk, sk) : X86.crypto_box_easy(c, m, mlen, n, pk, sk))
                : _isMac64 ? MacOsxX64.crypto_box_easy(c, m, mlen, n, pk, sk) : X64.crypto_box_easy(c, m, mlen, n, pk, sk);
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
                ? (_isArm ? Arm.crypto_box_open_easy(m, c, clen, n, pk, sk) : X86.crypto_box_open_easy(m, c, clen, n, pk, sk))
                : _isMac64 ? MacOsxX64.crypto_box_open_easy(m, c, clen, n, pk, sk) : X64.crypto_box_open_easy(m, c, clen, n, pk, sk);
        }

        public static int crypto_sign_detached(
            byte* sig,
            ulong* siglen,
            byte* m,
            ulong mlen,
            byte* sk)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_sign_detached(sig, siglen, m, mlen, sk) : X86.crypto_sign_detached(sig, siglen, m, mlen, sk))
                : _isMac64 ? MacOsxX64.crypto_sign_detached(sig, siglen, m, mlen, sk) : X64.crypto_sign_detached(sig, siglen, m, mlen, sk);
        }

        public static int crypto_sign_verify_detached(
            byte* sig,
            byte* m,
            ulong mlen,
            byte* pk)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_sign_verify_detached(sig, m, mlen, pk) : X86.crypto_sign_verify_detached(sig, m, mlen, pk))
                : _isMac64 ? MacOsxX64.crypto_sign_verify_detached(sig, m, mlen, pk) : X64.crypto_sign_verify_detached(sig, m, mlen, pk);
        }

        public static UIntPtr crypto_sign_publickeybytes()
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_sign_publickeybytes() : X86.crypto_sign_publickeybytes())
                : _isMac64 ? MacOsxX64.crypto_sign_publickeybytes() : X64.crypto_sign_publickeybytes();
        }

        public static UIntPtr crypto_sign_secretkeybytes()
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_sign_secretkeybytes() : X86.crypto_sign_secretkeybytes())
                : _isMac64 ? MacOsxX64.crypto_sign_secretkeybytes() : X64.crypto_sign_secretkeybytes();
        }

        public static UIntPtr crypto_sign_bytes()
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_sign_bytes() : X86.crypto_sign_bytes())
                : _isMac64 ? MacOsxX64.crypto_sign_bytes() : X64.crypto_sign_bytes();
        }

        public static int crypto_sign_keypair(
            byte* pk,
            byte* sk)
        {
            return _is32bits
                ? (_isArm ? Arm.crypto_sign_keypair(pk, sk) : X86.crypto_sign_keypair(pk, sk))
                : _isMac64 ? MacOsxX64.crypto_sign_keypair(pk, sk) : X64.crypto_sign_keypair(pk, sk);
        }

        public static void sodium_memzero(
            byte* pnt,
            UIntPtr len)
        {
            if (_is32bits)
            {
                if (_isArm)
                    Arm.sodium_memzero(pnt, len);
                else
                    X86.sodium_memzero(pnt, len);
            }
            else
            {
                if (_isMac64)
                    MacOsxX64.sodium_memzero(pnt, len);
                else
                    X64.sodium_memzero(pnt, len);
            }
        }



        private static class X64
        {
            private const string LIB_SODIUM = "libsodium.x64.so";

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
                            $"{LIB_SODIUM} probably contains the wrong version or not usable on the current platform. Try installing libsodium from https://download.libsodium.org/libsodium/releases",
                            dllNotFoundEx);

                    }
                }
            }

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_bytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_sign_statebytes();


            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_statebytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_init(void* /* crypto_generichash_state */ state);

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_update(void* /* crypto_generichash_state */ state,
                byte* m,
                ulong mlen);

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_final_create(void* /* crypto_generichash_state */ state,
                byte* sig,
                ulong* siglen_p,
                byte* sk);

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
            public static extern UIntPtr crypto_stream_xchacha20_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_stream_xchacha20_noncebytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_kdf_keybytes();

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
            public static extern int crypto_aead_xchacha20poly1305_ietf_encrypt(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_decrypt(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(
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
            public static extern int crypto_box_keypair(
                byte* pk,
                byte* sk);

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
            public static extern int sodium_memcmp(
                byte* b1,
                byte* b2,
                UIntPtr len);

            [DllImport(LIB_SODIUM)]
            public static extern void sodium_memzero(
                byte* pnt,
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
        } // ReSharper disable once InconsistentNaming

        private static class X86
        {
            private const string LIB_SODIUM = "libsodium.x86.so";

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
                            $"{LIB_SODIUM} probably contains the wrong version or not usable on the current platform. Try installing libsodium from https://download.libsodium.org/libsodium/releases",
                            dllNotFoundEx);

                    }
                }
            }

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_bytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_sign_statebytes();
            
            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_statebytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_init(void* /* crypto_generichash_state */ state);

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_update(void* /* crypto_generichash_state */ state,
                byte* m,
                ulong mlen);

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_final_create(void* /* crypto_generichash_state */ state,
                byte* sig,
                ulong* siglen_p,
                byte* sk);

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
            public static extern UIntPtr crypto_stream_xchacha20_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_stream_xchacha20_noncebytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_kdf_keybytes();

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
            public static extern int crypto_aead_xchacha20poly1305_ietf_encrypt(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_decrypt(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(
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
            public static extern int crypto_box_keypair(
                byte* pk,
                byte* sk);

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
            public static extern int sodium_memcmp(
                byte* b1,
                byte* b2,
                UIntPtr len);

            [DllImport(LIB_SODIUM)]
            public static extern void sodium_memzero(
                byte* pnt,
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
        }

        private static class Arm
        {
            private const string LIB_SODIUM = "libsodium.arm.so";

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
                            $"{LIB_SODIUM} probably contains the wrong version or not usable on the current platform. Try installing package libsodium or libsodium-12 or libsodium-18",
                            dllNotFoundEx);

                    }
                }
            }

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_bytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_sign_statebytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_statebytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_init(void* /* crypto_generichash_state */ state);

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_update(void* /* crypto_generichash_state */ state,
                byte* m,
                ulong mlen);

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_final_create(void* /* crypto_generichash_state */ state,
                byte* sig,
                ulong* siglen_p,
                byte* sk);

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
            public static extern UIntPtr crypto_stream_xchacha20_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_stream_xchacha20_noncebytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_kdf_keybytes();

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
            public static extern int crypto_aead_xchacha20poly1305_ietf_encrypt(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_decrypt(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(
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
            public static extern int crypto_box_keypair(
                byte* pk,
                byte* sk);

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
            public static extern int sodium_memcmp(
                byte* b1,
                byte* b2,
                UIntPtr len);

            [DllImport(LIB_SODIUM)]
            public static extern void sodium_memzero(
                byte* pnt,
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
        }

        private static class MacOsxX64
        {
            private const string LIB_SODIUM = "libsodium";

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
                            $"{LIB_SODIUM} probably contains the wrong version or not usable on the current platform. Try `brew install libsodium` and re-run server.",
                            dllNotFoundEx);

                    }
                }
            }

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_bytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_sign_statebytes();


            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_statebytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_generichash_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_init(void* /* crypto_generichash_state */ state);

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_update(void* /* crypto_generichash_state */ state,
                byte* m,
                ulong mlen);

            [DllImport(LIB_SODIUM)]
            public static extern int crypto_sign_final_create(void* /* crypto_generichash_state */ state,
                byte* sig,
                ulong* siglen_p,
                byte* sk);

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
            public static extern UIntPtr crypto_stream_xchacha20_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_stream_xchacha20_noncebytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes();

            [DllImport(LIB_SODIUM)]
            public static extern UIntPtr crypto_kdf_keybytes();

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
            public static extern int crypto_aead_xchacha20poly1305_ietf_encrypt(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_decrypt(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(
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
            public static extern int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(
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
            public static extern int crypto_box_keypair(
                byte* pk,
                byte* sk);

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
            public static extern int sodium_memcmp(
                byte* b1,
                byte* b2,
                UIntPtr len);

            [DllImport(LIB_SODIUM)]
            public static extern void sodium_memzero(
                byte* pnt,
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
        } // ReSharper disable once InconsistentNaming
    }
}
