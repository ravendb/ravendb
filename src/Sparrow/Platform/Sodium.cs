using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Utils;
using System.Threading;

namespace Sparrow.Platform
{
#if NETCOREAPP3_1_OR_GREATER
    public static unsafe partial class Sodium
    {
        static Sodium()
        {
            try
            {
                var rc = sodium_init();
                if (rc != 0)
                    throw new InvalidOperationException("Unable to initialize sodium, error code: " + rc);
                crypto_sign_publickeybytes(); // test libsodium functionality
            }
            catch (Exception ex)
            {
                var errString = $"{LIBSODIUM} version might be invalid, missing or not usable on current platform.";

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    errString += " Initialization error could also be caused by missing 'Microsoft Visual C++ 2015 Redistributable Package' (or newer). It can be downloaded from https://support.microsoft.com/en-us/help/2977003/the-latest-supported-visual-c-downloads.";

                errString += $" Arch: {RuntimeInformation.OSArchitecture}, OSDesc: {RuntimeInformation.OSDescription}";

                throw new IncorrectDllException(errString, ex);
            }
        }

        private const string LIBSODIUM = "libsodium";

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_kdf_keybytes();

        [DllImport(LIBSODIUM)]
        public static extern int sodium_init();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_generichash_bytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_sign_statebytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_generichash_keybytes();

        [DllImport(LIBSODIUM)]
        public static extern int crypto_sign_init(void* /* crypto_sign_state  */ state);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_sign_update(void* /* crypto_generichash_state */ state, byte* m, ulong mlen);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_sign_final_create(void* /* crypto_generichash_state */ state, byte* sig, ulong* siglen_p, byte* sk);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_generichash_init(void* /* crypto_generichash_state */ state, byte* key, UIntPtr keylen, UIntPtr outlen);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_generichash_update(void* /* crypto_generichash_state */ state, byte* @in, ulong inlen);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_generichash_final(void* /* crypto_generichash_state */ state, byte* @out, UIntPtr outlen);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_kx_keypair(byte* pk, byte* sk);

        [DllImport(LIBSODIUM)]
        public static extern void randombytes_buf(byte* buffer, UIntPtr size);

        [DllImport(LIBSODIUM)]
        public static extern void crypto_kdf_keygen(byte* masterkey);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_kdf_derive_from_key(byte* subkey, UIntPtr subkeylen, ulong subkeyid, byte* ctx, byte* key);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_stream_xchacha20_xor_ic(byte* c, byte* m, ulong mlen, byte* n, ulong ic, byte* k);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_aead_xchacha20poly1305_ietf_encrypt(byte* c, ulong* clen, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_aead_xchacha20poly1305_ietf_decrypt(byte* m, ulong* mlen, byte* nsec, byte* c, ulong clen, byte* ad, ulong adlen, byte* npub, byte* k);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(byte* c, byte* mac, ulong* maclen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(byte* m, byte* nsec, byte* c, ulong clen, byte* mac, byte* ad, ulong adlen, byte* npub, byte* k);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_box_seal(byte* c, byte* m, ulong mlen, byte* pk);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_box_seal_open(byte* m, byte* c, ulong clen, byte* pk, byte* sk);

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_stream_xchacha20_keybytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_stream_xchacha20_noncebytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_box_sealbytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_box_secretkeybytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_kx_secretkeybytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_kx_publickeybytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_generichash_bytes_max();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_box_publickeybytes();

        [DllImport(LIBSODIUM)]
        public static extern int crypto_box_keypair(byte* pk, byte* sk);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_generichash(byte* @out, UIntPtr outlen, byte* @in, ulong inlen, byte* key, UIntPtr keylen);

        [DllImport(LIBSODIUM)]
        public static extern int sodium_memcmp(byte* b, byte* vh, UIntPtr verifiedHashLength);

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_box_macbytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_box_noncebytes();

        [DllImport(LIBSODIUM)]
        public static extern int crypto_kx_client_session_keys(byte* rx, byte* tx, byte* client_pk, byte* client_sk, byte* server_pk);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_kx_server_session_keys(byte* rx, byte* tx, byte* server_pk, byte* server_sk, byte* client_pk);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_box_easy(byte* c, byte* m, ulong mlen, byte* n, byte* pk, byte* sk);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_box_open_easy(byte* m, byte* c, ulong clen, byte* n, byte* pk, byte* sk);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_sign_detached(byte* sig, ulong* siglen, byte* m, ulong mlen, byte* sk);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_sign_verify_detached(byte* sig, byte* m, ulong mlen, byte* pk);

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_sign_bytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_sign_publickeybytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_sign_secretkeybytes();

        [DllImport(LIBSODIUM)]
        public static extern int crypto_sign_keypair(byte* pk, byte* sk);

        [DllImport(LIBSODIUM)]
        public static extern void sodium_memzero(byte* pnt, UIntPtr len);

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_generichash_statebytes();

        [DllImport(LIBSODIUM)]
        public static extern void crypto_secretstream_xchacha20poly1305_keygen(byte* k);

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_secretstream_xchacha20poly1305_keybytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_secretstream_xchacha20poly1305_statebytes();

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_secretstream_xchacha20poly1305_headerbytes();

        [DllImport(LIBSODIUM)]
        public static extern int crypto_secretstream_xchacha20poly1305_init_push(byte* state, byte* header, byte* k);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_secretstream_xchacha20poly1305_push(byte* state, byte* c, ulong* clen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte tag);

        [DllImport(LIBSODIUM)]
        public static extern byte crypto_secretstream_xchacha20poly1305_tag_final();

        [DllImport(LIBSODIUM)]
        public static extern int crypto_secretstream_xchacha20poly1305_init_pull(byte* state, byte* header, byte* k);

        [DllImport(LIBSODIUM)]
        public static extern int crypto_secretstream_xchacha20poly1305_pull(byte* state, byte* m, ulong* mlen_p, byte* tag_p, byte* c, ulong clen, byte* ad, ulong adlen);

        [DllImport(LIBSODIUM)]
        public static extern UIntPtr crypto_secretstream_xchacha20poly1305_abytes();

        [DllImport(LIBSODIUM)]
        public static extern int sodium_munlock(byte* addr, UIntPtr len);

        [DllImport(LIBSODIUM)]
        public static extern int sodium_mlock(byte* addr, UIntPtr len);

        private static long _lockedBytes;

        public static long LockedBytes => _lockedBytes;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Lock(byte* addr, UIntPtr len)
        {
            var r = sodium_mlock(addr, len);
            if (r != 0)
                return r;

            Interlocked.Add(ref _lockedBytes, (long)len);
            return 0;
    }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Unlock(byte* addr, UIntPtr len)
        {
            var r = sodium_munlock(addr, len);
            if (r != 0)
                return r;

            Interlocked.Add(ref _lockedBytes, -(long)len);
            return 0;
}
    }
#endif
}
