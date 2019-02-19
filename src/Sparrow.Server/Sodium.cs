using System;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow.Server.Platform;

namespace Sparrow.Server
{
    public static unsafe partial class Sodium
    {
        static Sodium()
        {
            var toFilename = LIBSODIUM;
            string fromFilename;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                if (RuntimeInformation.ProcessArchitecture != Architecture.Arm &&
                    RuntimeInformation.ProcessArchitecture != Architecture.Arm64)
                {
                    fromFilename = Environment.Is64BitProcess ? $"{toFilename}.linux.x64.so" : $"{toFilename}.linux.x86.so";
                    toFilename += ".so";
                }
                else
                {
                    fromFilename = Environment.Is64BitProcess ? $"{toFilename}.arm.64.so" : $"{toFilename}.arm.32.so";
                    toFilename += ".so";
                }
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                fromFilename = Environment.Is64BitProcess ? $"{toFilename}.mac.x64.dylib" : $"{toFilename}.mac.x86.dylib";
                // in mac we are not : `toFilename += ".so";` as DllImport doesn't assume .so nor .dylib by default
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                fromFilename = Environment.Is64BitProcess ? $"{toFilename}.win.x64.dll" : $"{toFilename}.win.x86.dll";
                toFilename += ".dll";
            }
            else
            {
                throw new NotSupportedException("Not supported platform - no Linux/OSX/Windows is detected ");
            }

            try
            {
                var toTime = DateTime.MinValue.Ticks;
                if (File.Exists(toFilename))
                    toTime = new FileInfo(toFilename).CreationTime.Ticks;

                if (File.Exists(fromFilename) &&
                    new FileInfo(fromFilename).CreationTime.Ticks > toTime)
                    File.Copy(fromFilename, toFilename, overwrite: true);
            }
            catch (IOException e)
            {
                throw new IOException(
                    $"Cannot copy {fromFilename} to {toFilename}, make sure appropriate {toFilename} to your platform architecture exists in Raven.Server executable folder",
                    e);
            }

            try
            {
                var rc = sodium_init();
                if (rc != 0)
                    throw new InvalidOperationException("Unable to initialize sodium, error code: " + rc);
            }
            catch (DllNotFoundException dllNotFoundEx)
            {
                // make sure the lib file is not there (this exception might pop when incorrect libsodium lib is does exists)
                if (File.Exists(LIBSODIUM))
                {
                    throw new IncorrectDllException(
                        $"{LIBSODIUM} probably contains the wrong version or not usable on the current platform. Try installing {LIBSODIUM} and re-run server.",
                        dllNotFoundEx);

                }
            }

            // test for existence of libsodium:
            try
            {
                crypto_sign_publickeybytes();
            }
            catch (IncorrectDllException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new IncorrectDllException($"Make sure {LIBSODIUM} is installed on your Linux OS",
                    ex);
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
    }
}
