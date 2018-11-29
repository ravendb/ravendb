using System;
using System.IO;
using Sparrow.Platform;
using System.Runtime.InteropServices;

namespace Sparrow
{
    public static unsafe partial class Sodium
    {
		#region Public API
 
		public static UIntPtr crypto_kdf_keybytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_kdf_keybytes();
			 }
             else
			 {
			 	return Windows.crypto_kdf_keybytes();
			 }
		}

		public static int sodium_init()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.sodium_init();
			 }
             else
			 {
			 	return Windows.sodium_init();
			 }
		}

		public static UIntPtr crypto_generichash_bytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_generichash_bytes();
			 }
             else
			 {
			 	return Windows.crypto_generichash_bytes();
			 }
		}

		public static UIntPtr crypto_sign_statebytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_sign_statebytes();
			 }
             else
			 {
			 	return Windows.crypto_sign_statebytes();
			 }
		}

		public static UIntPtr crypto_generichash_keybytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_generichash_keybytes();
			 }
             else
			 {
			 	return Windows.crypto_generichash_keybytes();
			 }
		}

		public static int crypto_sign_init(void* /* crypto_sign_state  */ state)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_sign_init(state);
			 }
             else
			 {
			 	return Windows.crypto_sign_init(state);
			 }
		}

		public static int crypto_sign_update(void* /* crypto_generichash_state */ state, byte* m, ulong mlen)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_sign_update(state, m, mlen);
			 }
             else
			 {
			 	return Windows.crypto_sign_update(state, m, mlen);
			 }
		}

		public static int crypto_sign_final_create(void* /* crypto_generichash_state */ state, byte* sig, ulong* siglen_p, byte* sk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_sign_final_create(state, sig, siglen_p, sk);
			 }
             else
			 {
			 	return Windows.crypto_sign_final_create(state, sig, siglen_p, sk);
			 }
		}

		public static int crypto_generichash_init(void* /* crypto_generichash_state */ state, byte* key, UIntPtr keylen, UIntPtr outlen)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_generichash_init(state, key, keylen, outlen);
			 }
             else
			 {
			 	return Windows.crypto_generichash_init(state, key, keylen, outlen);
			 }
		}

		public static int crypto_generichash_update(void* /* crypto_generichash_state */ state, byte* @in, ulong inlen)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_generichash_update(state, @in, inlen);
			 }
             else
			 {
			 	return Windows.crypto_generichash_update(state, @in, inlen);
			 }
		}

		public static int crypto_generichash_final(void* /* crypto_generichash_state */ state, byte* @out, UIntPtr outlen)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_generichash_final(state, @out, outlen);
			 }
             else
			 {
			 	return Windows.crypto_generichash_final(state, @out, outlen);
			 }
		}

		public static int crypto_kx_keypair(byte* pk, byte* sk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_kx_keypair(pk, sk);
			 }
             else
			 {
			 	return Windows.crypto_kx_keypair(pk, sk);
			 }
		}

		public static void randombytes_buf(byte* buffer, UIntPtr size)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				Posix.randombytes_buf(buffer, size);
			 }
             else
			 {
			 	Windows.randombytes_buf(buffer, size);
			 }
		}

		public static void crypto_kdf_keygen(byte* masterkey)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				Posix.crypto_kdf_keygen(masterkey);
			 }
             else
			 {
			 	Windows.crypto_kdf_keygen(masterkey);
			 }
		}

		public static int crypto_kdf_derive_from_key(byte* subkey, UIntPtr subkeylen, ulong subkeyid, byte* ctx, byte* key)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
			 }
             else
			 {
			 	return Windows.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
			 }
		}

		public static int crypto_stream_xchacha20_xor_ic(byte* c, byte* m, ulong mlen, byte* n, ulong ic, byte* k)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
			 }
             else
			 {
			 	return Windows.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
			 }
		}

		public static int crypto_aead_xchacha20poly1305_ietf_encrypt(byte* c, ulong* clen, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
			 }
             else
			 {
			 	return Windows.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
			 }
		}

		public static int crypto_aead_xchacha20poly1305_ietf_decrypt(byte* m, ulong* mlen, byte* nsec, byte* c, ulong clen, byte* ad, ulong adlen, byte* npub, byte* k)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
			 }
             else
			 {
			 	return Windows.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
			 }
		}

		public static int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(byte* c, byte* mac, ulong* maclen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
			 }
             else
			 {
			 	return Windows.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
			 }
		}

		public static int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(byte* m, byte* nsec, byte* c, ulong clen, byte* mac, byte* ad, ulong adlen, byte* npub, byte* k)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);
			 }
             else
			 {
			 	return Windows.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);
			 }
		}

		public static int crypto_box_seal(byte* c, byte* m, ulong mlen, byte* pk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_box_seal(c, m, mlen, pk);
			 }
             else
			 {
			 	return Windows.crypto_box_seal(c, m, mlen, pk);
			 }
		}

		public static int crypto_box_seal_open(byte* m, byte* c, ulong clen, byte* pk, byte* sk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_box_seal_open(m, c, clen, pk, sk);
			 }
             else
			 {
			 	return Windows.crypto_box_seal_open(m, c, clen, pk, sk);
			 }
		}

		public static UIntPtr crypto_stream_xchacha20_keybytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_stream_xchacha20_keybytes();
			 }
             else
			 {
			 	return Windows.crypto_stream_xchacha20_keybytes();
			 }
		}

		public static UIntPtr crypto_stream_xchacha20_noncebytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_stream_xchacha20_noncebytes();
			 }
             else
			 {
			 	return Windows.crypto_stream_xchacha20_noncebytes();
			 }
		}

		public static UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_aead_xchacha20poly1305_ietf_keybytes();
			 }
             else
			 {
			 	return Windows.crypto_aead_xchacha20poly1305_ietf_keybytes();
			 }
		}

		public static UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_aead_xchacha20poly1305_ietf_npubbytes();
			 }
             else
			 {
			 	return Windows.crypto_aead_xchacha20poly1305_ietf_npubbytes();
			 }
		}

		public static UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_aead_xchacha20poly1305_ietf_abytes();
			 }
             else
			 {
			 	return Windows.crypto_aead_xchacha20poly1305_ietf_abytes();
			 }
		}

		public static UIntPtr crypto_box_sealbytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_box_sealbytes();
			 }
             else
			 {
			 	return Windows.crypto_box_sealbytes();
			 }
		}

		public static UIntPtr crypto_box_secretkeybytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_box_secretkeybytes();
			 }
             else
			 {
			 	return Windows.crypto_box_secretkeybytes();
			 }
		}

		public static UIntPtr crypto_kx_secretkeybytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_kx_secretkeybytes();
			 }
             else
			 {
			 	return Windows.crypto_kx_secretkeybytes();
			 }
		}

		public static UIntPtr crypto_kx_publickeybytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_kx_publickeybytes();
			 }
             else
			 {
			 	return Windows.crypto_kx_publickeybytes();
			 }
		}

		public static UIntPtr crypto_generichash_bytes_max()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_generichash_bytes_max();
			 }
             else
			 {
			 	return Windows.crypto_generichash_bytes_max();
			 }
		}

		public static UIntPtr crypto_box_publickeybytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_box_publickeybytes();
			 }
             else
			 {
			 	return Windows.crypto_box_publickeybytes();
			 }
		}

		public static int crypto_box_keypair(byte* pk, byte* sk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_box_keypair(pk, sk);
			 }
             else
			 {
			 	return Windows.crypto_box_keypair(pk, sk);
			 }
		}

		public static int crypto_generichash(byte* @out, UIntPtr outlen, byte* @in, ulong inlen, byte* key, UIntPtr keylen)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
			 }
             else
			 {
			 	return Windows.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
			 }
		}

		public static int sodium_memcmp(byte* b, byte* vh, UIntPtr verifiedHashLength)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.sodium_memcmp(b, vh, verifiedHashLength);
			 }
             else
			 {
			 	return Windows.sodium_memcmp(b, vh, verifiedHashLength);
			 }
		}

		public static UIntPtr crypto_box_macbytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_box_macbytes();
			 }
             else
			 {
			 	return Windows.crypto_box_macbytes();
			 }
		}

		public static UIntPtr crypto_box_noncebytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_box_noncebytes();
			 }
             else
			 {
			 	return Windows.crypto_box_noncebytes();
			 }
		}

		public static int crypto_kx_client_session_keys(byte* rx, byte* tx, byte* client_pk, byte* client_sk, byte* server_pk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk);
			 }
             else
			 {
			 	return Windows.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk);
			 }
		}

		public static int crypto_kx_server_session_keys(byte* rx, byte* tx, byte* server_pk, byte* server_sk, byte* client_pk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk);
			 }
             else
			 {
			 	return Windows.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk);
			 }
		}

		public static int crypto_box_easy(byte* c, byte* m, ulong mlen, byte* n, byte* pk, byte* sk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_box_easy(c, m, mlen, n, pk, sk);
			 }
             else
			 {
			 	return Windows.crypto_box_easy(c, m, mlen, n, pk, sk);
			 }
		}

		public static int crypto_box_open_easy(byte* m, byte* c, ulong clen, byte* n, byte* pk, byte* sk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_box_open_easy(m, c, clen, n, pk, sk);
			 }
             else
			 {
			 	return Windows.crypto_box_open_easy(m, c, clen, n, pk, sk);
			 }
		}

		public static int crypto_sign_detached(byte* sig, ulong* siglen, byte* m, ulong mlen, byte* sk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_sign_detached(sig, siglen, m, mlen, sk);
			 }
             else
			 {
			 	return Windows.crypto_sign_detached(sig, siglen, m, mlen, sk);
			 }
		}

		public static int crypto_sign_verify_detached(byte* sig, byte* m, ulong mlen, byte* pk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_sign_verify_detached(sig, m, mlen, pk);
			 }
             else
			 {
			 	return Windows.crypto_sign_verify_detached(sig, m, mlen, pk);
			 }
		}

		public static UIntPtr crypto_sign_bytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_sign_bytes();
			 }
             else
			 {
			 	return Windows.crypto_sign_bytes();
			 }
		}

		public static UIntPtr crypto_sign_publickeybytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_sign_publickeybytes();
			 }
             else
			 {
			 	return Windows.crypto_sign_publickeybytes();
			 }
		}

		public static UIntPtr crypto_sign_secretkeybytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_sign_secretkeybytes();
			 }
             else
			 {
			 	return Windows.crypto_sign_secretkeybytes();
			 }
		}

		public static int crypto_sign_keypair(byte* pk, byte* sk)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_sign_keypair(pk, sk);
			 }
             else
			 {
			 	return Windows.crypto_sign_keypair(pk, sk);
			 }
		}

		public static void sodium_memzero(byte* pnt, UIntPtr len)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				Posix.sodium_memzero(pnt, len);
			 }
             else
			 {
			 	Windows.sodium_memzero(pnt, len);
			 }
		}

		public static UIntPtr crypto_generichash_statebytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_generichash_statebytes();
			 }
             else
			 {
			 	return Windows.crypto_generichash_statebytes();
			 }
		}

		public static void crypto_secretstream_xchacha20poly1305_keygen(byte* k)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				Posix.crypto_secretstream_xchacha20poly1305_keygen(k);
			 }
             else
			 {
			 	Windows.crypto_secretstream_xchacha20poly1305_keygen(k);
			 }
		}

		public static UIntPtr crypto_secretstream_xchacha20poly1305_keybytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_secretstream_xchacha20poly1305_keybytes();
			 }
             else
			 {
			 	return Windows.crypto_secretstream_xchacha20poly1305_keybytes();
			 }
		}

		public static UIntPtr crypto_secretstream_xchacha20poly1305_statebytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_secretstream_xchacha20poly1305_statebytes();
			 }
             else
			 {
			 	return Windows.crypto_secretstream_xchacha20poly1305_statebytes();
			 }
		}

		public static UIntPtr crypto_secretstream_xchacha20poly1305_headerbytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_secretstream_xchacha20poly1305_headerbytes();
			 }
             else
			 {
			 	return Windows.crypto_secretstream_xchacha20poly1305_headerbytes();
			 }
		}

		public static int crypto_secretstream_xchacha20poly1305_init_push(byte* state, byte* header, byte* k)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_secretstream_xchacha20poly1305_init_push(state, header, k);
			 }
             else
			 {
			 	return Windows.crypto_secretstream_xchacha20poly1305_init_push(state, header, k);
			 }
		}

		public static int crypto_secretstream_xchacha20poly1305_push(byte* state, byte* c, ulong* clen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte tag)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_secretstream_xchacha20poly1305_push(state, c, clen_p, m, mlen, ad, adlen, tag);
			 }
             else
			 {
			 	return Windows.crypto_secretstream_xchacha20poly1305_push(state, c, clen_p, m, mlen, ad, adlen, tag);
			 }
		}

		public static byte crypto_secretstream_xchacha20poly1305_tag_final()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_secretstream_xchacha20poly1305_tag_final();
			 }
             else
			 {
			 	return Windows.crypto_secretstream_xchacha20poly1305_tag_final();
			 }
		}

		public static int crypto_secretstream_xchacha20poly1305_init_pull(byte* state, byte* header, byte* k)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_secretstream_xchacha20poly1305_init_pull(state, header, k);
			 }
             else
			 {
			 	return Windows.crypto_secretstream_xchacha20poly1305_init_pull(state, header, k);
			 }
		}

		public static int crypto_secretstream_xchacha20poly1305_pull(byte *state, byte *m, ulong* mlen_p, byte *tag_p,byte* c, ulong clen,byte*ad, ulong adlen)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_secretstream_xchacha20poly1305_pull(state, m, mlen_p, tag_p, c, clen, ad, adlen);
			 }
             else
			 {
			 	return Windows.crypto_secretstream_xchacha20poly1305_pull(state, m, mlen_p, tag_p, c, clen, ad, adlen);
			 }
		}

		public static UIntPtr crypto_secretstream_xchacha20poly1305_abytes()
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.crypto_secretstream_xchacha20poly1305_abytes();
			 }
             else
			 {
			 	return Windows.crypto_secretstream_xchacha20poly1305_abytes();
			 }
		}

		public static int sodium_munlock(byte* addr, UIntPtr len)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.sodium_munlock(addr, len);
			 }
             else
			 {
			 	return Windows.sodium_munlock(addr, len);
			 }
		}

		public static int sodium_mlock(byte* addr, UIntPtr len)
		{
			 if(PlatformDetails.RunningOnPosix)
			 {
				return Posix.sodium_mlock(addr, len);
			 }
             else
			 {
			 	return Windows.sodium_mlock(addr, len);
			 }
		}


		#endregion

		#region Posix

		private class Posix
		{
			private static readonly bool _is32bits;
			private static readonly bool _isArm;
			private static bool _isMac64;

			static Posix()
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

			public static UIntPtr crypto_kdf_keybytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_kdf_keybytes();
					}
					else
					{
						return X86.crypto_kdf_keybytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_kdf_keybytes();
					}
					else
					{
						return X64.crypto_kdf_keybytes();
					}
				 }
			}

			public static int sodium_init()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.sodium_init();
					}
					else
					{
						return X86.sodium_init();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.sodium_init();
					}
					else
					{
						return X64.sodium_init();
					}
				 }
			}

			public static UIntPtr crypto_generichash_bytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_generichash_bytes();
					}
					else
					{
						return X86.crypto_generichash_bytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_generichash_bytes();
					}
					else
					{
						return X64.crypto_generichash_bytes();
					}
				 }
			}

			public static UIntPtr crypto_sign_statebytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_sign_statebytes();
					}
					else
					{
						return X86.crypto_sign_statebytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_sign_statebytes();
					}
					else
					{
						return X64.crypto_sign_statebytes();
					}
				 }
			}

			public static UIntPtr crypto_generichash_keybytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_generichash_keybytes();
					}
					else
					{
						return X86.crypto_generichash_keybytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_generichash_keybytes();
					}
					else
					{
						return X64.crypto_generichash_keybytes();
					}
				 }
			}

			public static int crypto_sign_init(void* /* crypto_sign_state  */ state)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_sign_init(state);
					}
					else
					{
						return X86.crypto_sign_init(state);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_sign_init(state);
					}
					else
					{
						return X64.crypto_sign_init(state);
					}
				 }
			}

			public static int crypto_sign_update(void* /* crypto_generichash_state */ state, byte* m, ulong mlen)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_sign_update(state, m, mlen);
					}
					else
					{
						return X86.crypto_sign_update(state, m, mlen);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_sign_update(state, m, mlen);
					}
					else
					{
						return X64.crypto_sign_update(state, m, mlen);
					}
				 }
			}

			public static int crypto_sign_final_create(void* /* crypto_generichash_state */ state, byte* sig, ulong* siglen_p, byte* sk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_sign_final_create(state, sig, siglen_p, sk);
					}
					else
					{
						return X86.crypto_sign_final_create(state, sig, siglen_p, sk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_sign_final_create(state, sig, siglen_p, sk);
					}
					else
					{
						return X64.crypto_sign_final_create(state, sig, siglen_p, sk);
					}
				 }
			}

			public static int crypto_generichash_init(void* /* crypto_generichash_state */ state, byte* key, UIntPtr keylen, UIntPtr outlen)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_generichash_init(state, key, keylen, outlen);
					}
					else
					{
						return X86.crypto_generichash_init(state, key, keylen, outlen);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_generichash_init(state, key, keylen, outlen);
					}
					else
					{
						return X64.crypto_generichash_init(state, key, keylen, outlen);
					}
				 }
			}

			public static int crypto_generichash_update(void* /* crypto_generichash_state */ state, byte* @in, ulong inlen)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_generichash_update(state, @in, inlen);
					}
					else
					{
						return X86.crypto_generichash_update(state, @in, inlen);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_generichash_update(state, @in, inlen);
					}
					else
					{
						return X64.crypto_generichash_update(state, @in, inlen);
					}
				 }
			}

			public static int crypto_generichash_final(void* /* crypto_generichash_state */ state, byte* @out, UIntPtr outlen)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_generichash_final(state, @out, outlen);
					}
					else
					{
						return X86.crypto_generichash_final(state, @out, outlen);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_generichash_final(state, @out, outlen);
					}
					else
					{
						return X64.crypto_generichash_final(state, @out, outlen);
					}
				 }
			}

			public static int crypto_kx_keypair(byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_kx_keypair(pk, sk);
					}
					else
					{
						return X86.crypto_kx_keypair(pk, sk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_kx_keypair(pk, sk);
					}
					else
					{
						return X64.crypto_kx_keypair(pk, sk);
					}
				 }
			}

			public static void randombytes_buf(byte* buffer, UIntPtr size)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						Arm.randombytes_buf(buffer, size);
					}
					else
					{
						X86.randombytes_buf(buffer, size);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						MacOsxX64.randombytes_buf(buffer, size);
					}
					else
					{
						X64.randombytes_buf(buffer, size);
					}
				 }
			}

			public static void crypto_kdf_keygen(byte* masterkey)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						Arm.crypto_kdf_keygen(masterkey);
					}
					else
					{
						X86.crypto_kdf_keygen(masterkey);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						MacOsxX64.crypto_kdf_keygen(masterkey);
					}
					else
					{
						X64.crypto_kdf_keygen(masterkey);
					}
				 }
			}

			public static int crypto_kdf_derive_from_key(byte* subkey, UIntPtr subkeylen, ulong subkeyid, byte* ctx, byte* key)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
					}
					else
					{
						return X86.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
					}
					else
					{
						return X64.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
					}
				 }
			}

			public static int crypto_stream_xchacha20_xor_ic(byte* c, byte* m, ulong mlen, byte* n, ulong ic, byte* k)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
					}
					else
					{
						return X86.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
					}
					else
					{
						return X64.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
					}
				 }
			}

			public static int crypto_aead_xchacha20poly1305_ietf_encrypt(byte* c, ulong* clen, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
					}
					else
					{
						return X86.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
					}
					else
					{
						return X64.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
					}
				 }
			}

			public static int crypto_aead_xchacha20poly1305_ietf_decrypt(byte* m, ulong* mlen, byte* nsec, byte* c, ulong clen, byte* ad, ulong adlen, byte* npub, byte* k)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
					}
					else
					{
						return X86.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
					}
					else
					{
						return X64.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
					}
				 }
			}

			public static int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(byte* c, byte* mac, ulong* maclen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
					}
					else
					{
						return X86.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
					}
					else
					{
						return X64.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
					}
				 }
			}

			public static int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(byte* m, byte* nsec, byte* c, ulong clen, byte* mac, byte* ad, ulong adlen, byte* npub, byte* k)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);
					}
					else
					{
						return X86.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);
					}
					else
					{
						return X64.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);
					}
				 }
			}

			public static int crypto_box_seal(byte* c, byte* m, ulong mlen, byte* pk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_box_seal(c, m, mlen, pk);
					}
					else
					{
						return X86.crypto_box_seal(c, m, mlen, pk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_box_seal(c, m, mlen, pk);
					}
					else
					{
						return X64.crypto_box_seal(c, m, mlen, pk);
					}
				 }
			}

			public static int crypto_box_seal_open(byte* m, byte* c, ulong clen, byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_box_seal_open(m, c, clen, pk, sk);
					}
					else
					{
						return X86.crypto_box_seal_open(m, c, clen, pk, sk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_box_seal_open(m, c, clen, pk, sk);
					}
					else
					{
						return X64.crypto_box_seal_open(m, c, clen, pk, sk);
					}
				 }
			}

			public static UIntPtr crypto_stream_xchacha20_keybytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_stream_xchacha20_keybytes();
					}
					else
					{
						return X86.crypto_stream_xchacha20_keybytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_stream_xchacha20_keybytes();
					}
					else
					{
						return X64.crypto_stream_xchacha20_keybytes();
					}
				 }
			}

			public static UIntPtr crypto_stream_xchacha20_noncebytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_stream_xchacha20_noncebytes();
					}
					else
					{
						return X86.crypto_stream_xchacha20_noncebytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_stream_xchacha20_noncebytes();
					}
					else
					{
						return X64.crypto_stream_xchacha20_noncebytes();
					}
				 }
			}

			public static UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_aead_xchacha20poly1305_ietf_keybytes();
					}
					else
					{
						return X86.crypto_aead_xchacha20poly1305_ietf_keybytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_aead_xchacha20poly1305_ietf_keybytes();
					}
					else
					{
						return X64.crypto_aead_xchacha20poly1305_ietf_keybytes();
					}
				 }
			}

			public static UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_aead_xchacha20poly1305_ietf_npubbytes();
					}
					else
					{
						return X86.crypto_aead_xchacha20poly1305_ietf_npubbytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_aead_xchacha20poly1305_ietf_npubbytes();
					}
					else
					{
						return X64.crypto_aead_xchacha20poly1305_ietf_npubbytes();
					}
				 }
			}

			public static UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_aead_xchacha20poly1305_ietf_abytes();
					}
					else
					{
						return X86.crypto_aead_xchacha20poly1305_ietf_abytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_aead_xchacha20poly1305_ietf_abytes();
					}
					else
					{
						return X64.crypto_aead_xchacha20poly1305_ietf_abytes();
					}
				 }
			}

			public static UIntPtr crypto_box_sealbytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_box_sealbytes();
					}
					else
					{
						return X86.crypto_box_sealbytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_box_sealbytes();
					}
					else
					{
						return X64.crypto_box_sealbytes();
					}
				 }
			}

			public static UIntPtr crypto_box_secretkeybytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_box_secretkeybytes();
					}
					else
					{
						return X86.crypto_box_secretkeybytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_box_secretkeybytes();
					}
					else
					{
						return X64.crypto_box_secretkeybytes();
					}
				 }
			}

			public static UIntPtr crypto_kx_secretkeybytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_kx_secretkeybytes();
					}
					else
					{
						return X86.crypto_kx_secretkeybytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_kx_secretkeybytes();
					}
					else
					{
						return X64.crypto_kx_secretkeybytes();
					}
				 }
			}

			public static UIntPtr crypto_kx_publickeybytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_kx_publickeybytes();
					}
					else
					{
						return X86.crypto_kx_publickeybytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_kx_publickeybytes();
					}
					else
					{
						return X64.crypto_kx_publickeybytes();
					}
				 }
			}

			public static UIntPtr crypto_generichash_bytes_max()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_generichash_bytes_max();
					}
					else
					{
						return X86.crypto_generichash_bytes_max();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_generichash_bytes_max();
					}
					else
					{
						return X64.crypto_generichash_bytes_max();
					}
				 }
			}

			public static UIntPtr crypto_box_publickeybytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_box_publickeybytes();
					}
					else
					{
						return X86.crypto_box_publickeybytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_box_publickeybytes();
					}
					else
					{
						return X64.crypto_box_publickeybytes();
					}
				 }
			}

			public static int crypto_box_keypair(byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_box_keypair(pk, sk);
					}
					else
					{
						return X86.crypto_box_keypair(pk, sk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_box_keypair(pk, sk);
					}
					else
					{
						return X64.crypto_box_keypair(pk, sk);
					}
				 }
			}

			public static int crypto_generichash(byte* @out, UIntPtr outlen, byte* @in, ulong inlen, byte* key, UIntPtr keylen)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
					}
					else
					{
						return X86.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
					}
					else
					{
						return X64.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
					}
				 }
			}

			public static int sodium_memcmp(byte* b, byte* vh, UIntPtr verifiedHashLength)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.sodium_memcmp(b, vh, verifiedHashLength);
					}
					else
					{
						return X86.sodium_memcmp(b, vh, verifiedHashLength);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.sodium_memcmp(b, vh, verifiedHashLength);
					}
					else
					{
						return X64.sodium_memcmp(b, vh, verifiedHashLength);
					}
				 }
			}

			public static UIntPtr crypto_box_macbytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_box_macbytes();
					}
					else
					{
						return X86.crypto_box_macbytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_box_macbytes();
					}
					else
					{
						return X64.crypto_box_macbytes();
					}
				 }
			}

			public static UIntPtr crypto_box_noncebytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_box_noncebytes();
					}
					else
					{
						return X86.crypto_box_noncebytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_box_noncebytes();
					}
					else
					{
						return X64.crypto_box_noncebytes();
					}
				 }
			}

			public static int crypto_kx_client_session_keys(byte* rx, byte* tx, byte* client_pk, byte* client_sk, byte* server_pk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk);
					}
					else
					{
						return X86.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk);
					}
					else
					{
						return X64.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk);
					}
				 }
			}

			public static int crypto_kx_server_session_keys(byte* rx, byte* tx, byte* server_pk, byte* server_sk, byte* client_pk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk);
					}
					else
					{
						return X86.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk);
					}
					else
					{
						return X64.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk);
					}
				 }
			}

			public static int crypto_box_easy(byte* c, byte* m, ulong mlen, byte* n, byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_box_easy(c, m, mlen, n, pk, sk);
					}
					else
					{
						return X86.crypto_box_easy(c, m, mlen, n, pk, sk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_box_easy(c, m, mlen, n, pk, sk);
					}
					else
					{
						return X64.crypto_box_easy(c, m, mlen, n, pk, sk);
					}
				 }
			}

			public static int crypto_box_open_easy(byte* m, byte* c, ulong clen, byte* n, byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_box_open_easy(m, c, clen, n, pk, sk);
					}
					else
					{
						return X86.crypto_box_open_easy(m, c, clen, n, pk, sk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_box_open_easy(m, c, clen, n, pk, sk);
					}
					else
					{
						return X64.crypto_box_open_easy(m, c, clen, n, pk, sk);
					}
				 }
			}

			public static int crypto_sign_detached(byte* sig, ulong* siglen, byte* m, ulong mlen, byte* sk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_sign_detached(sig, siglen, m, mlen, sk);
					}
					else
					{
						return X86.crypto_sign_detached(sig, siglen, m, mlen, sk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_sign_detached(sig, siglen, m, mlen, sk);
					}
					else
					{
						return X64.crypto_sign_detached(sig, siglen, m, mlen, sk);
					}
				 }
			}

			public static int crypto_sign_verify_detached(byte* sig, byte* m, ulong mlen, byte* pk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_sign_verify_detached(sig, m, mlen, pk);
					}
					else
					{
						return X86.crypto_sign_verify_detached(sig, m, mlen, pk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_sign_verify_detached(sig, m, mlen, pk);
					}
					else
					{
						return X64.crypto_sign_verify_detached(sig, m, mlen, pk);
					}
				 }
			}

			public static UIntPtr crypto_sign_bytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_sign_bytes();
					}
					else
					{
						return X86.crypto_sign_bytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_sign_bytes();
					}
					else
					{
						return X64.crypto_sign_bytes();
					}
				 }
			}

			public static UIntPtr crypto_sign_publickeybytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_sign_publickeybytes();
					}
					else
					{
						return X86.crypto_sign_publickeybytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_sign_publickeybytes();
					}
					else
					{
						return X64.crypto_sign_publickeybytes();
					}
				 }
			}

			public static UIntPtr crypto_sign_secretkeybytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_sign_secretkeybytes();
					}
					else
					{
						return X86.crypto_sign_secretkeybytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_sign_secretkeybytes();
					}
					else
					{
						return X64.crypto_sign_secretkeybytes();
					}
				 }
			}

			public static int crypto_sign_keypair(byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_sign_keypair(pk, sk);
					}
					else
					{
						return X86.crypto_sign_keypair(pk, sk);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_sign_keypair(pk, sk);
					}
					else
					{
						return X64.crypto_sign_keypair(pk, sk);
					}
				 }
			}

			public static void sodium_memzero(byte* pnt, UIntPtr len)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						Arm.sodium_memzero(pnt, len);
					}
					else
					{
						X86.sodium_memzero(pnt, len);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						MacOsxX64.sodium_memzero(pnt, len);
					}
					else
					{
						X64.sodium_memzero(pnt, len);
					}
				 }
			}

			public static UIntPtr crypto_generichash_statebytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_generichash_statebytes();
					}
					else
					{
						return X86.crypto_generichash_statebytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_generichash_statebytes();
					}
					else
					{
						return X64.crypto_generichash_statebytes();
					}
				 }
			}

			public static void crypto_secretstream_xchacha20poly1305_keygen(byte* k)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						Arm.crypto_secretstream_xchacha20poly1305_keygen(k);
					}
					else
					{
						X86.crypto_secretstream_xchacha20poly1305_keygen(k);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						MacOsxX64.crypto_secretstream_xchacha20poly1305_keygen(k);
					}
					else
					{
						X64.crypto_secretstream_xchacha20poly1305_keygen(k);
					}
				 }
			}

			public static UIntPtr crypto_secretstream_xchacha20poly1305_keybytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_secretstream_xchacha20poly1305_keybytes();
					}
					else
					{
						return X86.crypto_secretstream_xchacha20poly1305_keybytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_secretstream_xchacha20poly1305_keybytes();
					}
					else
					{
						return X64.crypto_secretstream_xchacha20poly1305_keybytes();
					}
				 }
			}

			public static UIntPtr crypto_secretstream_xchacha20poly1305_statebytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_secretstream_xchacha20poly1305_statebytes();
					}
					else
					{
						return X86.crypto_secretstream_xchacha20poly1305_statebytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_secretstream_xchacha20poly1305_statebytes();
					}
					else
					{
						return X64.crypto_secretstream_xchacha20poly1305_statebytes();
					}
				 }
			}

			public static UIntPtr crypto_secretstream_xchacha20poly1305_headerbytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_secretstream_xchacha20poly1305_headerbytes();
					}
					else
					{
						return X86.crypto_secretstream_xchacha20poly1305_headerbytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_secretstream_xchacha20poly1305_headerbytes();
					}
					else
					{
						return X64.crypto_secretstream_xchacha20poly1305_headerbytes();
					}
				 }
			}

			public static int crypto_secretstream_xchacha20poly1305_init_push(byte* state, byte* header, byte* k)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_secretstream_xchacha20poly1305_init_push(state, header, k);
					}
					else
					{
						return X86.crypto_secretstream_xchacha20poly1305_init_push(state, header, k);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_secretstream_xchacha20poly1305_init_push(state, header, k);
					}
					else
					{
						return X64.crypto_secretstream_xchacha20poly1305_init_push(state, header, k);
					}
				 }
			}

			public static int crypto_secretstream_xchacha20poly1305_push(byte* state, byte* c, ulong* clen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte tag)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_secretstream_xchacha20poly1305_push(state, c, clen_p, m, mlen, ad, adlen, tag);
					}
					else
					{
						return X86.crypto_secretstream_xchacha20poly1305_push(state, c, clen_p, m, mlen, ad, adlen, tag);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_secretstream_xchacha20poly1305_push(state, c, clen_p, m, mlen, ad, adlen, tag);
					}
					else
					{
						return X64.crypto_secretstream_xchacha20poly1305_push(state, c, clen_p, m, mlen, ad, adlen, tag);
					}
				 }
			}

			public static byte crypto_secretstream_xchacha20poly1305_tag_final()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_secretstream_xchacha20poly1305_tag_final();
					}
					else
					{
						return X86.crypto_secretstream_xchacha20poly1305_tag_final();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_secretstream_xchacha20poly1305_tag_final();
					}
					else
					{
						return X64.crypto_secretstream_xchacha20poly1305_tag_final();
					}
				 }
			}

			public static int crypto_secretstream_xchacha20poly1305_init_pull(byte* state, byte* header, byte* k)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_secretstream_xchacha20poly1305_init_pull(state, header, k);
					}
					else
					{
						return X86.crypto_secretstream_xchacha20poly1305_init_pull(state, header, k);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_secretstream_xchacha20poly1305_init_pull(state, header, k);
					}
					else
					{
						return X64.crypto_secretstream_xchacha20poly1305_init_pull(state, header, k);
					}
				 }
			}

			public static int crypto_secretstream_xchacha20poly1305_pull(byte *state, byte *m, ulong* mlen_p, byte *tag_p,byte* c, ulong clen,byte*ad, ulong adlen)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_secretstream_xchacha20poly1305_pull(state, m, mlen_p, tag_p, c, clen, ad, adlen);
					}
					else
					{
						return X86.crypto_secretstream_xchacha20poly1305_pull(state, m, mlen_p, tag_p, c, clen, ad, adlen);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_secretstream_xchacha20poly1305_pull(state, m, mlen_p, tag_p, c, clen, ad, adlen);
					}
					else
					{
						return X64.crypto_secretstream_xchacha20poly1305_pull(state, m, mlen_p, tag_p, c, clen, ad, adlen);
					}
				 }
			}

			public static UIntPtr crypto_secretstream_xchacha20poly1305_abytes()
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.crypto_secretstream_xchacha20poly1305_abytes();
					}
					else
					{
						return X86.crypto_secretstream_xchacha20poly1305_abytes();
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.crypto_secretstream_xchacha20poly1305_abytes();
					}
					else
					{
						return X64.crypto_secretstream_xchacha20poly1305_abytes();
					}
				 }
			}

			public static int sodium_munlock(byte* addr, UIntPtr len)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.sodium_munlock(addr, len);
					}
					else
					{
						return X86.sodium_munlock(addr, len);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.sodium_munlock(addr, len);
					}
					else
					{
						return X64.sodium_munlock(addr, len);
					}
				 }
			}

			public static int sodium_mlock(byte* addr, UIntPtr len)
			{
				 if(_is32bits)
				 {
					if(_isArm)
					{
						return Arm.sodium_mlock(addr, len);
					}
					else
					{
						return X86.sodium_mlock(addr, len);
					}
				 }
				 else
				 {
			 		if(_isMac64)
					{
						return MacOsxX64.sodium_mlock(addr, len);
					}
					else
					{
						return X64.sodium_mlock(addr, len);
					}
				 }
			}

			#region Mac OSX 64
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
				public extern static UIntPtr crypto_kdf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_init();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_init(void* /* crypto_sign_state  */ state);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_update(void* /* crypto_generichash_state */ state, byte* m, ulong mlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_final_create(void* /* crypto_generichash_state */ state, byte* sig, ulong* siglen_p, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_init(void* /* crypto_generichash_state */ state, byte* key, UIntPtr keylen, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_update(void* /* crypto_generichash_state */ state, byte* @in, ulong inlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_final(void* /* crypto_generichash_state */ state, byte* @out, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void randombytes_buf(byte* buffer, UIntPtr size);

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_kdf_keygen(byte* masterkey);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kdf_derive_from_key(byte* subkey, UIntPtr subkeylen, ulong subkeyid, byte* ctx, byte* key);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_stream_xchacha20_xor_ic(byte* c, byte* m, ulong mlen, byte* n, ulong ic, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt(byte* c, ulong* clen, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt(byte* m, ulong* mlen, byte* nsec, byte* c, ulong clen, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(byte* c, byte* mac, ulong* maclen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(byte* m, byte* nsec, byte* c, ulong clen, byte* mac, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal(byte* c, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal_open(byte* m, byte* c, ulong clen, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_sealbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes_max();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash(byte* @out, UIntPtr outlen, byte* @in, ulong inlen, byte* key, UIntPtr keylen);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_memcmp(byte* b, byte* vh, UIntPtr verifiedHashLength);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_macbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_client_session_keys(byte* rx, byte* tx, byte* client_pk, byte* client_sk, byte* server_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_server_session_keys(byte* rx, byte* tx, byte* server_pk, byte* server_sk, byte* client_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_easy(byte* c, byte* m, ulong mlen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_open_easy(byte* m, byte* c, ulong clen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_detached(byte* sig, ulong* siglen, byte* m, ulong mlen, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_verify_detached(byte* sig, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void sodium_memzero(byte* pnt, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_secretstream_xchacha20poly1305_keygen(byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_headerbytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_push(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_push(byte* state, byte* c, ulong* clen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte tag);

				[DllImport(LIB_SODIUM)]
				public extern static byte crypto_secretstream_xchacha20poly1305_tag_final();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_pull(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_pull(byte *state, byte *m, ulong* mlen_p, byte *tag_p,byte* c, ulong clen,byte*ad, ulong adlen);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_munlock(byte* addr, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_mlock(byte* addr, UIntPtr len);

			}
			#endregion

			#region X64
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
				public extern static UIntPtr crypto_kdf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_init();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_init(void* /* crypto_sign_state  */ state);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_update(void* /* crypto_generichash_state */ state, byte* m, ulong mlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_final_create(void* /* crypto_generichash_state */ state, byte* sig, ulong* siglen_p, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_init(void* /* crypto_generichash_state */ state, byte* key, UIntPtr keylen, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_update(void* /* crypto_generichash_state */ state, byte* @in, ulong inlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_final(void* /* crypto_generichash_state */ state, byte* @out, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void randombytes_buf(byte* buffer, UIntPtr size);

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_kdf_keygen(byte* masterkey);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kdf_derive_from_key(byte* subkey, UIntPtr subkeylen, ulong subkeyid, byte* ctx, byte* key);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_stream_xchacha20_xor_ic(byte* c, byte* m, ulong mlen, byte* n, ulong ic, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt(byte* c, ulong* clen, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt(byte* m, ulong* mlen, byte* nsec, byte* c, ulong clen, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(byte* c, byte* mac, ulong* maclen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(byte* m, byte* nsec, byte* c, ulong clen, byte* mac, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal(byte* c, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal_open(byte* m, byte* c, ulong clen, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_sealbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes_max();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash(byte* @out, UIntPtr outlen, byte* @in, ulong inlen, byte* key, UIntPtr keylen);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_memcmp(byte* b, byte* vh, UIntPtr verifiedHashLength);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_macbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_client_session_keys(byte* rx, byte* tx, byte* client_pk, byte* client_sk, byte* server_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_server_session_keys(byte* rx, byte* tx, byte* server_pk, byte* server_sk, byte* client_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_easy(byte* c, byte* m, ulong mlen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_open_easy(byte* m, byte* c, ulong clen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_detached(byte* sig, ulong* siglen, byte* m, ulong mlen, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_verify_detached(byte* sig, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void sodium_memzero(byte* pnt, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_secretstream_xchacha20poly1305_keygen(byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_headerbytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_push(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_push(byte* state, byte* c, ulong* clen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte tag);

				[DllImport(LIB_SODIUM)]
				public extern static byte crypto_secretstream_xchacha20poly1305_tag_final();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_pull(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_pull(byte *state, byte *m, ulong* mlen_p, byte *tag_p,byte* c, ulong clen,byte*ad, ulong adlen);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_munlock(byte* addr, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_mlock(byte* addr, UIntPtr len);

			}
			#endregion

			#region x86

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
				public extern static UIntPtr crypto_kdf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_init();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_init(void* /* crypto_sign_state  */ state);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_update(void* /* crypto_generichash_state */ state, byte* m, ulong mlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_final_create(void* /* crypto_generichash_state */ state, byte* sig, ulong* siglen_p, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_init(void* /* crypto_generichash_state */ state, byte* key, UIntPtr keylen, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_update(void* /* crypto_generichash_state */ state, byte* @in, ulong inlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_final(void* /* crypto_generichash_state */ state, byte* @out, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void randombytes_buf(byte* buffer, UIntPtr size);

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_kdf_keygen(byte* masterkey);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kdf_derive_from_key(byte* subkey, UIntPtr subkeylen, ulong subkeyid, byte* ctx, byte* key);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_stream_xchacha20_xor_ic(byte* c, byte* m, ulong mlen, byte* n, ulong ic, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt(byte* c, ulong* clen, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt(byte* m, ulong* mlen, byte* nsec, byte* c, ulong clen, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(byte* c, byte* mac, ulong* maclen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(byte* m, byte* nsec, byte* c, ulong clen, byte* mac, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal(byte* c, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal_open(byte* m, byte* c, ulong clen, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_sealbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes_max();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash(byte* @out, UIntPtr outlen, byte* @in, ulong inlen, byte* key, UIntPtr keylen);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_memcmp(byte* b, byte* vh, UIntPtr verifiedHashLength);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_macbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_client_session_keys(byte* rx, byte* tx, byte* client_pk, byte* client_sk, byte* server_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_server_session_keys(byte* rx, byte* tx, byte* server_pk, byte* server_sk, byte* client_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_easy(byte* c, byte* m, ulong mlen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_open_easy(byte* m, byte* c, ulong clen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_detached(byte* sig, ulong* siglen, byte* m, ulong mlen, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_verify_detached(byte* sig, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void sodium_memzero(byte* pnt, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_secretstream_xchacha20poly1305_keygen(byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_headerbytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_push(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_push(byte* state, byte* c, ulong* clen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte tag);

				[DllImport(LIB_SODIUM)]
				public extern static byte crypto_secretstream_xchacha20poly1305_tag_final();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_pull(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_pull(byte *state, byte *m, ulong* mlen_p, byte *tag_p,byte* c, ulong clen,byte*ad, ulong adlen);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_munlock(byte* addr, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_mlock(byte* addr, UIntPtr len);

			}

			#endregion

			#region Arm
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
				public extern static UIntPtr crypto_kdf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_init();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_init(void* /* crypto_sign_state  */ state);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_update(void* /* crypto_generichash_state */ state, byte* m, ulong mlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_final_create(void* /* crypto_generichash_state */ state, byte* sig, ulong* siglen_p, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_init(void* /* crypto_generichash_state */ state, byte* key, UIntPtr keylen, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_update(void* /* crypto_generichash_state */ state, byte* @in, ulong inlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_final(void* /* crypto_generichash_state */ state, byte* @out, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void randombytes_buf(byte* buffer, UIntPtr size);

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_kdf_keygen(byte* masterkey);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kdf_derive_from_key(byte* subkey, UIntPtr subkeylen, ulong subkeyid, byte* ctx, byte* key);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_stream_xchacha20_xor_ic(byte* c, byte* m, ulong mlen, byte* n, ulong ic, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt(byte* c, ulong* clen, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt(byte* m, ulong* mlen, byte* nsec, byte* c, ulong clen, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(byte* c, byte* mac, ulong* maclen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(byte* m, byte* nsec, byte* c, ulong clen, byte* mac, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal(byte* c, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal_open(byte* m, byte* c, ulong clen, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_sealbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes_max();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash(byte* @out, UIntPtr outlen, byte* @in, ulong inlen, byte* key, UIntPtr keylen);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_memcmp(byte* b, byte* vh, UIntPtr verifiedHashLength);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_macbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_client_session_keys(byte* rx, byte* tx, byte* client_pk, byte* client_sk, byte* server_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_server_session_keys(byte* rx, byte* tx, byte* server_pk, byte* server_sk, byte* client_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_easy(byte* c, byte* m, ulong mlen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_open_easy(byte* m, byte* c, ulong clen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_detached(byte* sig, ulong* siglen, byte* m, ulong mlen, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_verify_detached(byte* sig, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void sodium_memzero(byte* pnt, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_secretstream_xchacha20poly1305_keygen(byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_headerbytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_push(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_push(byte* state, byte* c, ulong* clen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte tag);

				[DllImport(LIB_SODIUM)]
				public extern static byte crypto_secretstream_xchacha20poly1305_tag_final();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_pull(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_pull(byte *state, byte *m, ulong* mlen_p, byte *tag_p,byte* c, ulong clen,byte*ad, ulong adlen);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_munlock(byte* addr, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_mlock(byte* addr, UIntPtr len);

			}
			#endregion
		}

		#endregion
		
		#region Windows
		private class Windows
		{
			private const string ErrString = "'Microsoft Visual C++ 2015 Redistributable Package' (or newer). It can be downloaded from https://support.microsoft.com/en-us/help/2977003/the-latest-supported-visual-c-downloads";

			private static readonly bool _is32bits;

			static Windows()
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

			public static UIntPtr crypto_kdf_keybytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_kdf_keybytes();
				 }
				 else
				 {
			 		return X64.crypto_kdf_keybytes();
				 }
			}

			public static int sodium_init()
			{
				 if(_is32bits)
				 {
					return X86.sodium_init();
				 }
				 else
				 {
			 		return X64.sodium_init();
				 }
			}

			public static UIntPtr crypto_generichash_bytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_generichash_bytes();
				 }
				 else
				 {
			 		return X64.crypto_generichash_bytes();
				 }
			}

			public static UIntPtr crypto_sign_statebytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_sign_statebytes();
				 }
				 else
				 {
			 		return X64.crypto_sign_statebytes();
				 }
			}

			public static UIntPtr crypto_generichash_keybytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_generichash_keybytes();
				 }
				 else
				 {
			 		return X64.crypto_generichash_keybytes();
				 }
			}

			public static int crypto_sign_init(void* /* crypto_sign_state  */ state)
			{
				 if(_is32bits)
				 {
					return X86.crypto_sign_init(state);
				 }
				 else
				 {
			 		return X64.crypto_sign_init(state);
				 }
			}

			public static int crypto_sign_update(void* /* crypto_generichash_state */ state, byte* m, ulong mlen)
			{
				 if(_is32bits)
				 {
					return X86.crypto_sign_update(state, m, mlen);
				 }
				 else
				 {
			 		return X64.crypto_sign_update(state, m, mlen);
				 }
			}

			public static int crypto_sign_final_create(void* /* crypto_generichash_state */ state, byte* sig, ulong* siglen_p, byte* sk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_sign_final_create(state, sig, siglen_p, sk);
				 }
				 else
				 {
			 		return X64.crypto_sign_final_create(state, sig, siglen_p, sk);
				 }
			}

			public static int crypto_generichash_init(void* /* crypto_generichash_state */ state, byte* key, UIntPtr keylen, UIntPtr outlen)
			{
				 if(_is32bits)
				 {
					return X86.crypto_generichash_init(state, key, keylen, outlen);
				 }
				 else
				 {
			 		return X64.crypto_generichash_init(state, key, keylen, outlen);
				 }
			}

			public static int crypto_generichash_update(void* /* crypto_generichash_state */ state, byte* @in, ulong inlen)
			{
				 if(_is32bits)
				 {
					return X86.crypto_generichash_update(state, @in, inlen);
				 }
				 else
				 {
			 		return X64.crypto_generichash_update(state, @in, inlen);
				 }
			}

			public static int crypto_generichash_final(void* /* crypto_generichash_state */ state, byte* @out, UIntPtr outlen)
			{
				 if(_is32bits)
				 {
					return X86.crypto_generichash_final(state, @out, outlen);
				 }
				 else
				 {
			 		return X64.crypto_generichash_final(state, @out, outlen);
				 }
			}

			public static int crypto_kx_keypair(byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_kx_keypair(pk, sk);
				 }
				 else
				 {
			 		return X64.crypto_kx_keypair(pk, sk);
				 }
			}

			public static void randombytes_buf(byte* buffer, UIntPtr size)
			{
				 if(_is32bits)
				 {
					X86.randombytes_buf(buffer, size);
				 }
				 else
				 {
			 		X64.randombytes_buf(buffer, size);
				 }
			}

			public static void crypto_kdf_keygen(byte* masterkey)
			{
				 if(_is32bits)
				 {
					X86.crypto_kdf_keygen(masterkey);
				 }
				 else
				 {
			 		X64.crypto_kdf_keygen(masterkey);
				 }
			}

			public static int crypto_kdf_derive_from_key(byte* subkey, UIntPtr subkeylen, ulong subkeyid, byte* ctx, byte* key)
			{
				 if(_is32bits)
				 {
					return X86.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
				 }
				 else
				 {
			 		return X64.crypto_kdf_derive_from_key(subkey, subkeylen, subkeyid, ctx, key);
				 }
			}

			public static int crypto_stream_xchacha20_xor_ic(byte* c, byte* m, ulong mlen, byte* n, ulong ic, byte* k)
			{
				 if(_is32bits)
				 {
					return X86.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
				 }
				 else
				 {
			 		return X64.crypto_stream_xchacha20_xor_ic(c, m, mlen, n, ic, k);
				 }
			}

			public static int crypto_aead_xchacha20poly1305_ietf_encrypt(byte* c, ulong* clen, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k)
			{
				 if(_is32bits)
				 {
					return X86.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
				 }
				 else
				 {
			 		return X64.crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen, m, mlen, ad, adlen, nsec, npub, k);
				 }
			}

			public static int crypto_aead_xchacha20poly1305_ietf_decrypt(byte* m, ulong* mlen, byte* nsec, byte* c, ulong clen, byte* ad, ulong adlen, byte* npub, byte* k)
			{
				 if(_is32bits)
				 {
					return X86.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
				 }
				 else
				 {
			 		return X64.crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen, nsec, c, clen, ad, adlen, npub, k);
				 }
			}

			public static int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(byte* c, byte* mac, ulong* maclen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k)
			{
				 if(_is32bits)
				 {
					return X86.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
				 }
				 else
				 {
			 		return X64.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(c, mac, maclen_p, m, mlen, ad, adlen, nsec, npub, k);
				 }
			}

			public static int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(byte* m, byte* nsec, byte* c, ulong clen, byte* mac, byte* ad, ulong adlen, byte* npub, byte* k)
			{
				 if(_is32bits)
				 {
					return X86.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);
				 }
				 else
				 {
			 		return X64.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(m, nsec, c, clen, mac, ad, adlen, npub, k);
				 }
			}

			public static int crypto_box_seal(byte* c, byte* m, ulong mlen, byte* pk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_box_seal(c, m, mlen, pk);
				 }
				 else
				 {
			 		return X64.crypto_box_seal(c, m, mlen, pk);
				 }
			}

			public static int crypto_box_seal_open(byte* m, byte* c, ulong clen, byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_box_seal_open(m, c, clen, pk, sk);
				 }
				 else
				 {
			 		return X64.crypto_box_seal_open(m, c, clen, pk, sk);
				 }
			}

			public static UIntPtr crypto_stream_xchacha20_keybytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_stream_xchacha20_keybytes();
				 }
				 else
				 {
			 		return X64.crypto_stream_xchacha20_keybytes();
				 }
			}

			public static UIntPtr crypto_stream_xchacha20_noncebytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_stream_xchacha20_noncebytes();
				 }
				 else
				 {
			 		return X64.crypto_stream_xchacha20_noncebytes();
				 }
			}

			public static UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_aead_xchacha20poly1305_ietf_keybytes();
				 }
				 else
				 {
			 		return X64.crypto_aead_xchacha20poly1305_ietf_keybytes();
				 }
			}

			public static UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_aead_xchacha20poly1305_ietf_npubbytes();
				 }
				 else
				 {
			 		return X64.crypto_aead_xchacha20poly1305_ietf_npubbytes();
				 }
			}

			public static UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_aead_xchacha20poly1305_ietf_abytes();
				 }
				 else
				 {
			 		return X64.crypto_aead_xchacha20poly1305_ietf_abytes();
				 }
			}

			public static UIntPtr crypto_box_sealbytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_box_sealbytes();
				 }
				 else
				 {
			 		return X64.crypto_box_sealbytes();
				 }
			}

			public static UIntPtr crypto_box_secretkeybytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_box_secretkeybytes();
				 }
				 else
				 {
			 		return X64.crypto_box_secretkeybytes();
				 }
			}

			public static UIntPtr crypto_kx_secretkeybytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_kx_secretkeybytes();
				 }
				 else
				 {
			 		return X64.crypto_kx_secretkeybytes();
				 }
			}

			public static UIntPtr crypto_kx_publickeybytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_kx_publickeybytes();
				 }
				 else
				 {
			 		return X64.crypto_kx_publickeybytes();
				 }
			}

			public static UIntPtr crypto_generichash_bytes_max()
			{
				 if(_is32bits)
				 {
					return X86.crypto_generichash_bytes_max();
				 }
				 else
				 {
			 		return X64.crypto_generichash_bytes_max();
				 }
			}

			public static UIntPtr crypto_box_publickeybytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_box_publickeybytes();
				 }
				 else
				 {
			 		return X64.crypto_box_publickeybytes();
				 }
			}

			public static int crypto_box_keypair(byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_box_keypair(pk, sk);
				 }
				 else
				 {
			 		return X64.crypto_box_keypair(pk, sk);
				 }
			}

			public static int crypto_generichash(byte* @out, UIntPtr outlen, byte* @in, ulong inlen, byte* key, UIntPtr keylen)
			{
				 if(_is32bits)
				 {
					return X86.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
				 }
				 else
				 {
			 		return X64.crypto_generichash(@out, outlen, @in, inlen, key, keylen);
				 }
			}

			public static int sodium_memcmp(byte* b, byte* vh, UIntPtr verifiedHashLength)
			{
				 if(_is32bits)
				 {
					return X86.sodium_memcmp(b, vh, verifiedHashLength);
				 }
				 else
				 {
			 		return X64.sodium_memcmp(b, vh, verifiedHashLength);
				 }
			}

			public static UIntPtr crypto_box_macbytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_box_macbytes();
				 }
				 else
				 {
			 		return X64.crypto_box_macbytes();
				 }
			}

			public static UIntPtr crypto_box_noncebytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_box_noncebytes();
				 }
				 else
				 {
			 		return X64.crypto_box_noncebytes();
				 }
			}

			public static int crypto_kx_client_session_keys(byte* rx, byte* tx, byte* client_pk, byte* client_sk, byte* server_pk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk);
				 }
				 else
				 {
			 		return X64.crypto_kx_client_session_keys(rx, tx, client_pk, client_sk, server_pk);
				 }
			}

			public static int crypto_kx_server_session_keys(byte* rx, byte* tx, byte* server_pk, byte* server_sk, byte* client_pk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk);
				 }
				 else
				 {
			 		return X64.crypto_kx_server_session_keys(rx, tx, server_pk, server_sk, client_pk);
				 }
			}

			public static int crypto_box_easy(byte* c, byte* m, ulong mlen, byte* n, byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_box_easy(c, m, mlen, n, pk, sk);
				 }
				 else
				 {
			 		return X64.crypto_box_easy(c, m, mlen, n, pk, sk);
				 }
			}

			public static int crypto_box_open_easy(byte* m, byte* c, ulong clen, byte* n, byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_box_open_easy(m, c, clen, n, pk, sk);
				 }
				 else
				 {
			 		return X64.crypto_box_open_easy(m, c, clen, n, pk, sk);
				 }
			}

			public static int crypto_sign_detached(byte* sig, ulong* siglen, byte* m, ulong mlen, byte* sk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_sign_detached(sig, siglen, m, mlen, sk);
				 }
				 else
				 {
			 		return X64.crypto_sign_detached(sig, siglen, m, mlen, sk);
				 }
			}

			public static int crypto_sign_verify_detached(byte* sig, byte* m, ulong mlen, byte* pk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_sign_verify_detached(sig, m, mlen, pk);
				 }
				 else
				 {
			 		return X64.crypto_sign_verify_detached(sig, m, mlen, pk);
				 }
			}

			public static UIntPtr crypto_sign_bytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_sign_bytes();
				 }
				 else
				 {
			 		return X64.crypto_sign_bytes();
				 }
			}

			public static UIntPtr crypto_sign_publickeybytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_sign_publickeybytes();
				 }
				 else
				 {
			 		return X64.crypto_sign_publickeybytes();
				 }
			}

			public static UIntPtr crypto_sign_secretkeybytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_sign_secretkeybytes();
				 }
				 else
				 {
			 		return X64.crypto_sign_secretkeybytes();
				 }
			}

			public static int crypto_sign_keypair(byte* pk, byte* sk)
			{
				 if(_is32bits)
				 {
					return X86.crypto_sign_keypair(pk, sk);
				 }
				 else
				 {
			 		return X64.crypto_sign_keypair(pk, sk);
				 }
			}

			public static void sodium_memzero(byte* pnt, UIntPtr len)
			{
				 if(_is32bits)
				 {
					X86.sodium_memzero(pnt, len);
				 }
				 else
				 {
			 		X64.sodium_memzero(pnt, len);
				 }
			}

			public static UIntPtr crypto_generichash_statebytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_generichash_statebytes();
				 }
				 else
				 {
			 		return X64.crypto_generichash_statebytes();
				 }
			}

			public static void crypto_secretstream_xchacha20poly1305_keygen(byte* k)
			{
				 if(_is32bits)
				 {
					X86.crypto_secretstream_xchacha20poly1305_keygen(k);
				 }
				 else
				 {
			 		X64.crypto_secretstream_xchacha20poly1305_keygen(k);
				 }
			}

			public static UIntPtr crypto_secretstream_xchacha20poly1305_keybytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_secretstream_xchacha20poly1305_keybytes();
				 }
				 else
				 {
			 		return X64.crypto_secretstream_xchacha20poly1305_keybytes();
				 }
			}

			public static UIntPtr crypto_secretstream_xchacha20poly1305_statebytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_secretstream_xchacha20poly1305_statebytes();
				 }
				 else
				 {
			 		return X64.crypto_secretstream_xchacha20poly1305_statebytes();
				 }
			}

			public static UIntPtr crypto_secretstream_xchacha20poly1305_headerbytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_secretstream_xchacha20poly1305_headerbytes();
				 }
				 else
				 {
			 		return X64.crypto_secretstream_xchacha20poly1305_headerbytes();
				 }
			}

			public static int crypto_secretstream_xchacha20poly1305_init_push(byte* state, byte* header, byte* k)
			{
				 if(_is32bits)
				 {
					return X86.crypto_secretstream_xchacha20poly1305_init_push(state, header, k);
				 }
				 else
				 {
			 		return X64.crypto_secretstream_xchacha20poly1305_init_push(state, header, k);
				 }
			}

			public static int crypto_secretstream_xchacha20poly1305_push(byte* state, byte* c, ulong* clen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte tag)
			{
				 if(_is32bits)
				 {
					return X86.crypto_secretstream_xchacha20poly1305_push(state, c, clen_p, m, mlen, ad, adlen, tag);
				 }
				 else
				 {
			 		return X64.crypto_secretstream_xchacha20poly1305_push(state, c, clen_p, m, mlen, ad, adlen, tag);
				 }
			}

			public static byte crypto_secretstream_xchacha20poly1305_tag_final()
			{
				 if(_is32bits)
				 {
					return X86.crypto_secretstream_xchacha20poly1305_tag_final();
				 }
				 else
				 {
			 		return X64.crypto_secretstream_xchacha20poly1305_tag_final();
				 }
			}

			public static int crypto_secretstream_xchacha20poly1305_init_pull(byte* state, byte* header, byte* k)
			{
				 if(_is32bits)
				 {
					return X86.crypto_secretstream_xchacha20poly1305_init_pull(state, header, k);
				 }
				 else
				 {
			 		return X64.crypto_secretstream_xchacha20poly1305_init_pull(state, header, k);
				 }
			}

			public static int crypto_secretstream_xchacha20poly1305_pull(byte *state, byte *m, ulong* mlen_p, byte *tag_p,byte* c, ulong clen,byte*ad, ulong adlen)
			{
				 if(_is32bits)
				 {
					return X86.crypto_secretstream_xchacha20poly1305_pull(state, m, mlen_p, tag_p, c, clen, ad, adlen);
				 }
				 else
				 {
			 		return X64.crypto_secretstream_xchacha20poly1305_pull(state, m, mlen_p, tag_p, c, clen, ad, adlen);
				 }
			}

			public static UIntPtr crypto_secretstream_xchacha20poly1305_abytes()
			{
				 if(_is32bits)
				 {
					return X86.crypto_secretstream_xchacha20poly1305_abytes();
				 }
				 else
				 {
			 		return X64.crypto_secretstream_xchacha20poly1305_abytes();
				 }
			}

			public static int sodium_munlock(byte* addr, UIntPtr len)
			{
				 if(_is32bits)
				 {
					return X86.sodium_munlock(addr, len);
				 }
				 else
				 {
			 		return X64.sodium_munlock(addr, len);
				 }
			}

			public static int sodium_mlock(byte* addr, UIntPtr len)
			{
				 if(_is32bits)
				 {
					return X86.sodium_mlock(addr, len);
				 }
				 else
				 {
			 		return X64.sodium_mlock(addr, len);
				 }
			}


			#region x86

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
								$"{LIB_SODIUM} version might be invalid or not usable on current platform. Initialization error could also be caused by missing {ErrString}",
								dllNotFoundEx);
						}

						throw new DllNotFoundException(
							$"{LIB_SODIUM} is missing. Also make sure to have {ErrString}",
							dllNotFoundEx);
					}
					catch (Exception e)
					{
						throw new IncorrectDllException($"Error occured while trying to init {LIB_SODIUM}. Make sure existence of {ErrString}", e);
					}
				}

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kdf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_init();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_init(void* /* crypto_sign_state  */ state);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_update(void* /* crypto_generichash_state */ state, byte* m, ulong mlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_final_create(void* /* crypto_generichash_state */ state, byte* sig, ulong* siglen_p, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_init(void* /* crypto_generichash_state */ state, byte* key, UIntPtr keylen, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_update(void* /* crypto_generichash_state */ state, byte* @in, ulong inlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_final(void* /* crypto_generichash_state */ state, byte* @out, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void randombytes_buf(byte* buffer, UIntPtr size);

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_kdf_keygen(byte* masterkey);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kdf_derive_from_key(byte* subkey, UIntPtr subkeylen, ulong subkeyid, byte* ctx, byte* key);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_stream_xchacha20_xor_ic(byte* c, byte* m, ulong mlen, byte* n, ulong ic, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt(byte* c, ulong* clen, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt(byte* m, ulong* mlen, byte* nsec, byte* c, ulong clen, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(byte* c, byte* mac, ulong* maclen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(byte* m, byte* nsec, byte* c, ulong clen, byte* mac, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal(byte* c, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal_open(byte* m, byte* c, ulong clen, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_sealbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes_max();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash(byte* @out, UIntPtr outlen, byte* @in, ulong inlen, byte* key, UIntPtr keylen);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_memcmp(byte* b, byte* vh, UIntPtr verifiedHashLength);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_macbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_client_session_keys(byte* rx, byte* tx, byte* client_pk, byte* client_sk, byte* server_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_server_session_keys(byte* rx, byte* tx, byte* server_pk, byte* server_sk, byte* client_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_easy(byte* c, byte* m, ulong mlen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_open_easy(byte* m, byte* c, ulong clen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_detached(byte* sig, ulong* siglen, byte* m, ulong mlen, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_verify_detached(byte* sig, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void sodium_memzero(byte* pnt, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_secretstream_xchacha20poly1305_keygen(byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_headerbytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_push(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_push(byte* state, byte* c, ulong* clen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte tag);

				[DllImport(LIB_SODIUM)]
				public extern static byte crypto_secretstream_xchacha20poly1305_tag_final();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_pull(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_pull(byte *state, byte *m, ulong* mlen_p, byte *tag_p,byte* c, ulong clen,byte*ad, ulong adlen);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_munlock(byte* addr, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_mlock(byte* addr, UIntPtr len);

			}

			#endregion

			#region x64

			private class X64
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
								$"{LIB_SODIUM} version might be invalid or not usable on current platform. Initialization error could also be caused by missing {ErrString}",
								dllNotFoundEx);
						}

						throw new DllNotFoundException(
							$"{LIB_SODIUM} is missing. Also make sure to have {ErrString}",
							dllNotFoundEx);
					}
					catch (Exception e)
					{
						throw new IncorrectDllException($"Error occured while trying to init {LIB_SODIUM}. Make sure existence of {ErrString}", e);
					}
				}

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kdf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_init();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_init(void* /* crypto_sign_state  */ state);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_update(void* /* crypto_generichash_state */ state, byte* m, ulong mlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_final_create(void* /* crypto_generichash_state */ state, byte* sig, ulong* siglen_p, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_init(void* /* crypto_generichash_state */ state, byte* key, UIntPtr keylen, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_update(void* /* crypto_generichash_state */ state, byte* @in, ulong inlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash_final(void* /* crypto_generichash_state */ state, byte* @out, UIntPtr outlen);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void randombytes_buf(byte* buffer, UIntPtr size);

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_kdf_keygen(byte* masterkey);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kdf_derive_from_key(byte* subkey, UIntPtr subkeylen, ulong subkeyid, byte* ctx, byte* key);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_stream_xchacha20_xor_ic(byte* c, byte* m, ulong mlen, byte* n, ulong ic, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt(byte* c, ulong* clen, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt(byte* m, ulong* mlen, byte* nsec, byte* c, ulong clen, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_encrypt_detached(byte* c, byte* mac, ulong* maclen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte* nsec, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_aead_xchacha20poly1305_ietf_decrypt_detached(byte* m, byte* nsec, byte* c, ulong clen, byte* mac, byte* ad, ulong adlen, byte* npub, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal(byte* c, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_seal_open(byte* m, byte* c, ulong clen, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_stream_xchacha20_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_npubbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_aead_xchacha20poly1305_ietf_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_sealbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_kx_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_bytes_max();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_generichash(byte* @out, UIntPtr outlen, byte* @in, ulong inlen, byte* key, UIntPtr keylen);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_memcmp(byte* b, byte* vh, UIntPtr verifiedHashLength);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_macbytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_box_noncebytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_client_session_keys(byte* rx, byte* tx, byte* client_pk, byte* client_sk, byte* server_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_kx_server_session_keys(byte* rx, byte* tx, byte* server_pk, byte* server_sk, byte* client_pk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_easy(byte* c, byte* m, ulong mlen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_box_open_easy(byte* m, byte* c, ulong clen, byte* n, byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_detached(byte* sig, ulong* siglen, byte* m, ulong mlen, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_verify_detached(byte* sig, byte* m, ulong mlen, byte* pk);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_bytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_publickeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_sign_secretkeybytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_sign_keypair(byte* pk, byte* sk);

				[DllImport(LIB_SODIUM)]
				public extern static void sodium_memzero(byte* pnt, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_generichash_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static void crypto_secretstream_xchacha20poly1305_keygen(byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_keybytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_statebytes();

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_headerbytes();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_push(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_push(byte* state, byte* c, ulong* clen_p, byte* m, ulong mlen, byte* ad, ulong adlen, byte tag);

				[DllImport(LIB_SODIUM)]
				public extern static byte crypto_secretstream_xchacha20poly1305_tag_final();

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_init_pull(byte* state, byte* header, byte* k);

				[DllImport(LIB_SODIUM)]
				public extern static int crypto_secretstream_xchacha20poly1305_pull(byte *state, byte *m, ulong* mlen_p, byte *tag_p,byte* c, ulong clen,byte*ad, ulong adlen);

				[DllImport(LIB_SODIUM)]
				public extern static UIntPtr crypto_secretstream_xchacha20poly1305_abytes();

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_munlock(byte* addr, UIntPtr len);

				[DllImport(LIB_SODIUM)]
				public extern static int sodium_mlock(byte* addr, UIntPtr len);


		    }

			#endregion
		}
		#endregion
	}
}
