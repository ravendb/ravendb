using System;
using System.Security.Cryptography;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Native.ArrayBuffer;
using Jint.Native.Object;
using Jint.Native.TypedArray;
using Jint.Runtime.Descriptors;

namespace Raven.Server.Documents.Patch;

public sealed partial class ScriptRunner
{
    public sealed partial class SingleRun
    {
        private JsValue _uint8ArrayConstructor;

        private void InitializeCrypto()
        {
            _uint8ArrayConstructor = ScriptEngine.Evaluate("Uint8Array");

            var crypto = new JsObject(ScriptEngine);
            crypto.SetClfFunc("getRandomValues", Crypto_GetRandomValues);
            crypto.SetClfFunc("randomUUID", Crypto_RandomUUID);

            var subtle = new JsObject(ScriptEngine);
            subtle.SetClfFunc("digest", Crypto_Subtle_Digest);
            subtle.SetClfFunc("sign", Crypto_Subtle_Sign);
            subtle.SetClfFunc("verify", Crypto_Subtle_Verify);
            subtle.SetClfFunc("encrypt", Crypto_Subtle_Encrypt);
            subtle.SetClfFunc("decrypt", Crypto_Subtle_Decrypt);

            string[] notImplemented = ["generateKey", "deriveKey", "deriveBits", "importKey", "exportKey", "wrapKey", "unwrapKey"];
            foreach (var method in notImplemented)
            {
                subtle.SetClfFunc(method, (self, args) => throw new NotImplementedException($"crypto.subtle.{method} is not implemented"));
            }

            crypto.FastSetProperty("subtle", new PropertyDescriptor(subtle, false, false, false));
            ScriptEngine.SetValue("crypto", crypto);
        }

        private JsValue Crypto_RandomUUID(JsValue self, JsValue[] args)
        {
            // crypto.randomUUID(): string
            return Guid.NewGuid().ToString("D");
        }

        private JsValue Crypto_GetRandomValues(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.getRandomValues(typedArray: ArrayBufferView): ArrayBufferView";
            if (args.Length != 1)
                throw new ArgumentException($"{signature} requires 1 argument");

            var target = args[0];
            if (target.IsObject() == false || target.AsObject() is not JsTypedArray typedArray)
                throw new ArgumentException($"{signature}: Argument must be a TypedArray");

            var len = (int)typedArray.Length;

            var bytes = new byte[len];
            using (var rng = RandomNumberGenerator.Create())
            {
                rng.GetBytes(bytes);
            }

            for (int i = 0; i < len; i++)
            {
                typedArray[i] = (int)bytes[i];
            }

            return target;
        }

        private JsValue Crypto_Subtle_Digest(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.subtle.digest(algorithm: 'SHA-256' | 'SHA-384' | 'SHA-512', data: BufferSource | string): ArrayBuffer";
            if (args.Length != 2)
                throw new ArgumentException($"{signature} requires 2 arguments: algorithm, data");

            string algoName = GetAlgorithmName(args[0], signature);
            byte[] data = GetBytesFromJsValue(args[1], signature);
            var hash = algoName switch
            {
                "SHA-256" => SHA256.HashData(data),
                "SHA-384" => SHA384.HashData(data),
                "SHA-512" => SHA512.HashData(data),
                _ => throw new NotSupportedException($"{signature}: Algorithm '{algoName}' is not supported.")
            };

            return CreateArrayBuffer(hash);
        }

        private JsValue Crypto_Subtle_Sign(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.subtle.sign(algorithm: { name: 'HMAC', hash: 'SHA-256' | 'SHA-384' | 'SHA-512' }, key: BufferSource, data: BufferSource | string): ArrayBuffer";
            if (args.Length != 3)
                throw new ArgumentException($"{signature} requires 3 arguments");
            string algoName = GetAlgorithmName(args[0], signature);

            if (algoName.Equals("HMAC", StringComparison.OrdinalIgnoreCase) == false)
                throw new NotSupportedException($"{signature}: Algorithm '{algoName}' is not supported for signing.");

            byte[] keyData = GetKeyData(args[1], signature);
            byte[] data = GetBytesFromJsValue(args[2], signature);

            string hashName = "SHA-256";
            if (args[0].IsObject() && args[0].AsObject().TryGetValue("hash", out var hashVal) && hashVal.IsString())
            {
                hashName = hashVal.AsString();
            }

            var computedSignature = hashName switch
            {
                "SHA-256" => HMACSHA256.HashData(keyData, data),
                "SHA-384" => HMACSHA384.HashData(keyData, data),
                "SHA-512" => HMACSHA512.HashData(keyData, data),
                _ => throw new NotSupportedException($"{signature}: Unsupported hash for HMAC: " + hashName)
            };

            return CreateArrayBuffer(computedSignature);
        }

        private JsValue Crypto_Subtle_Verify(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.subtle.verify(algorithm: { name: 'HMAC', hash: 'SHA-256' | 'SHA-384' | 'SHA-512' }, key: BufferSource, signature: BufferSource, data: BufferSource | string): boolean";
            if (args.Length != 4)
                throw new ArgumentException($"{signature} requires 4 arguments");
            string algoName = GetAlgorithmName(args[0], signature);

            if (algoName.Equals("HMAC", StringComparison.OrdinalIgnoreCase) == false)
                throw new NotSupportedException($"{signature}: Algorithm '{algoName}' is not supported for verify.");

            byte[] keyData = GetKeyData(args[1], signature);
            byte[] signatureBytes = GetBytesFromJsValue(args[2], signature);
            byte[] data = GetBytesFromJsValue(args[3], signature);

            string hashName = "SHA-256";
            if (args[0].IsObject() && args[0].AsObject().TryGetValue("hash", out var hashVal) && hashVal.IsString())
            {
                hashName = hashVal.AsString();
            }

            var computed = hashName switch
            {
                "SHA-256" => HMACSHA256.HashData(keyData, data),
                "SHA-384" => HMACSHA384.HashData(keyData, data),
                "SHA-512" => HMACSHA512.HashData(keyData, data),
                _ => throw new NotSupportedException($"{signature}: Unsupported hash for HMAC: " + hashName)
            };

            bool valid = CryptographicOperations.FixedTimeEquals(computed, signatureBytes);
            return valid ? JsBoolean.True : JsBoolean.False;
        }

        private JsValue Crypto_Subtle_Encrypt(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.subtle.encrypt(algorithm: { name: 'AES-GCM', iv: BufferSource }, key: BufferSource, data: BufferSource | string): ArrayBuffer";
            if (args.Length != 3)
                throw new ArgumentException($"{signature} requires 3 arguments");

            string algoName = GetAlgorithmName(args[0], signature);

            if (algoName.Equals("AES-GCM", StringComparison.OrdinalIgnoreCase) == false)
                throw new NotSupportedException($"{signature}: Algorithm '{algoName}' is not supported for encryption.");

            // get iv
            byte[] iv = null;
            if (args[0].IsObject() && args[0].AsObject().TryGetValue("iv", out var ivVal))
                iv = GetBytesFromJsValue(ivVal, signature);
            if (iv == null)
                throw new ArgumentException($"{signature}: AES-GCM requires iv");

            byte[] key = GetKeyData(args[1], signature);
            byte[] plain = GetBytesFromJsValue(args[2], signature);

            const int tagSizeInBytes = 16;
            var result = new byte[plain.Length + tagSizeInBytes];
            Span<byte> buffer = result;

            using (var aes = new AesGcm(key, tagSizeInBytes))
            {
                aes.Encrypt(iv, plain, buffer.Slice(0, buffer.Length - tagSizeInBytes), buffer.Slice(buffer.Length - tagSizeInBytes));
            }

            return CreateArrayBuffer(result);
        }

        private JsValue Crypto_Subtle_Decrypt(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.subtle.decrypt(algorithm: { name: 'AES-GCM', iv: BufferSource }, key: BufferSource, data: BufferSource | string): ArrayBuffer";
            if (args.Length != 3)
                throw new ArgumentException($"{signature} requires 3 arguments");
            string algoName = GetAlgorithmName(args[0], signature);

            if (algoName.Equals("AES-GCM", StringComparison.OrdinalIgnoreCase) == false)
                throw new NotSupportedException($"{signature}: Algorithm '{algoName}' is not supported for decryption.");

            byte[] iv = null;
            if (args[0].IsObject() && args[0].AsObject().TryGetValue("iv", out var ivVal))
                iv = GetBytesFromJsValue(ivVal, signature);
            if (iv == null)
                throw new ArgumentException($"{signature}: AES-GCM requires iv");

            byte[] key = GetKeyData(args[1], signature);
            byte[] cipherWithTag = GetBytesFromJsValue(args[2], signature);

            const int tagSizeInBytes = 16;
            if (cipherWithTag.Length < tagSizeInBytes)
                throw new ArgumentException($"{signature}: Ciphertext too short/invalid");

            var plain = new byte[cipherWithTag.Length - tagSizeInBytes];

            using (var aes = new AesGcm(key, tagSizeInBytes))
            {
                ReadOnlySpan<byte> input = cipherWithTag;
                aes.Decrypt(iv, input.Slice(0, input.Length - tagSizeInBytes), input.Slice(input.Length - tagSizeInBytes), plain);
            }

            return CreateArrayBuffer(plain);
        }

        private string GetAlgorithmName(JsValue arg, string signature)
        {
            if (arg.IsString())
                return arg.AsString();
            if (arg.IsObject() && arg.AsObject().TryGetValue("name", out var name))
                return name.AsString();
            throw new ArgumentException($"{signature}: Invalid algorithm argument");
        }

        private byte[] GetKeyData(JsValue arg, string signature)
        {
            try
            {
                return GetBytesFromJsValue(arg, signature);
            }
            catch
            {
                throw new ArgumentException($"{signature}: Key must be a BufferSource (Raw key data)");
            }
        }

        private byte[] GetBytesFromJsValue(JsValue val, string signature)
        {
            if (val.IsString())
                return Encoding.UTF8.GetBytes(val.AsString());

            if (val.IsObject())
            {
                var obj = val.AsObject();
                if (obj is JsTypedArray tai)
                {
                    var len = (int)tai.Length;
                    var res = new byte[len];
                    for (int i = 0; i < len; i++)
                        res[i] = (byte)tai[i].AsNumber();
                    return res;
                }
                if (obj is JsArrayBuffer)
                {
                    var u8View = ScriptEngine.Construct(_uint8ArrayConstructor, [obj]);
                    return GetBytesFromJsValue(u8View, signature);
                }
            }

            throw new NotSupportedException($"{signature}: Unsupported value type for bytes: {val.Type}");
        }

        private JsValue CreateArrayBuffer(byte[] data)
        {
            var u8 = ScriptEngine.Construct(_uint8ArrayConstructor, [data.Length]);
            var typedArray = (JsTypedArray)u8.AsObject();
            for (int i = 0; i < data.Length; i++)
            {
                typedArray[i] = (int)data[i];
            }
            return u8.Get("buffer");
        }
    }
}
