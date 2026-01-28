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
            crypto.SetClfFunc("getRandomValuesBase64", Crypto_GetRandomValuesBase64);
            crypto.SetClfFunc("randomUUID", Crypto_RandomUUID);
            crypto.SetClfFunc("digest", Crypto_Digest);
            crypto.SetClfFunc("sign", Crypto_Sign);
            crypto.SetClfFunc("verify", Crypto_Verify);
            crypto.SetClfFunc("encryptAesGcm", Crypto_EncryptAesGcm);
            crypto.SetClfFunc("decryptAesGcm", Crypto_DecryptAesGcm);

            var subtle = new JsObject(ScriptEngine);
            string[] implementedMethods = [
                // implemented methods - as sync versions
                "digest", "sign", "verify", "encrypt", "decrypt", 
                // not implemented methods
                "generateKey", "deriveKey", "deriveBits", "importKey", "exportKey", "wrapKey", "unwrapKey"
            ];
            foreach (var method in implementedMethods)
            {
                subtle.SetClfFunc(method, (self, args) => throw new NotImplementedException(GetSubtleMethodNotImplementedMessage(method)));
            }

            crypto.FastSetProperty("subtle", new PropertyDescriptor(subtle, false, false, false));
            ScriptEngine.SetValue("crypto", crypto);
        }

        private string GetSubtleMethodNotImplementedMessage(string method)
        {
            return method switch
            {
                "digest" => "crypto.subtle.digest is not available. Use the sync crypto.digest instead: crypto.digest(algorithm: 'SHA-256' | 'SHA-384' | 'SHA-512', data: BufferSource | string): ArrayBuffer",
                "sign" => "crypto.subtle.sign is not available. Use the sync crypto.sign instead: crypto.sign(hash: 'SHA-256' | 'SHA-384' | 'SHA-512', key: BufferSource, data: BufferSource | string): string (base64)",
                "verify" => "crypto.subtle.verify is not available. Use the sync crypto.verify instead: crypto.verify(hash: 'SHA-256' | 'SHA-384' | 'SHA-512', key: BufferSource, signature: string (base64) | BufferSource, data: BufferSource | string): boolean",
                "encrypt" => "crypto.subtle.encrypt is not available. Use the sync crypto.encryptAesGcm instead: crypto.encryptAesGcm(iv: BufferSource | string (base64), key: BufferSource | string (base64), data: BufferSource | string): string (base64)",
                "decrypt" => "crypto.subtle.decrypt is not available. Use the sync crypto.decryptAesGcm instead: crypto.decryptAesGcm(iv: BufferSource | string (base64), key: BufferSource | string (base64), data: string (base64) | BufferSource, outputType: 'string' | 'raw'): string | ArrayBuffer",
                _ => $"crypto.subtle.{method} is not available"
            };
        }

        private JsValue Crypto_RandomUUID(JsValue self, JsValue[] args)
        {
            // crypto.randomUUID(): string
            return Guid.NewGuid().ToString("D");
        }

        private JsValue Crypto_GetRandomValuesBase64(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.getRandomValuesBase64(lenInBytes: number): string (base64)";
            if (args.Length != 1)
                throw new ArgumentException($"{signature} requires 1 argument");

            if (args[0].IsNumber() == false)
                throw new ArgumentException($"{signature}: Argument must be a number");

            var len = (int)args[0].AsNumber();
            if (len <= 0)
                throw new ArgumentException($"{signature}: Length must be greater than 0");

            var bytes = new byte[len];
            using (var rng = RandomNumberGenerator.Create())
            {
                rng.GetBytes(bytes);
            }

            return Convert.ToBase64String(bytes);
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

        private JsValue Crypto_Digest(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.digest(algorithm: 'SHA-256' | 'SHA-384' | 'SHA-512', data: BufferSource | string): ArrayBuffer";
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

        private JsValue Crypto_Sign(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.sign(hash: 'SHA-256' | 'SHA-384' | 'SHA-512', key: BufferSource, data: BufferSource | string): ArrayBuffer";
            if (args.Length != 3)
                throw new ArgumentException($"{signature} requires 3 arguments");
            
            if (args[0].IsString() == false)
                throw new ArgumentException($"{signature}: First argument must be a hash algorithm string ('SHA-256', 'SHA-384', or 'SHA-512')");
            
            string hashName = args[0].AsString();
            byte[] keyData = GetKeyData(args[1], signature);
            byte[] data = GetBytesFromJsValue(args[2], signature);

            var computedSignature = hashName switch
            {
                "SHA-256" => HMACSHA256.HashData(keyData, data),
                "SHA-384" => HMACSHA384.HashData(keyData, data),
                "SHA-512" => HMACSHA512.HashData(keyData, data),
                _ => throw new NotSupportedException($"{signature}: Unsupported hash algorithm '{hashName}'")
            };

            return Convert.ToBase64String(computedSignature);
        }

        private JsValue Crypto_Verify(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.verify(hash: 'SHA-256' | 'SHA-384' | 'SHA-512', key: BufferSource, signature: string (base64) | BufferSource, data: BufferSource | string): boolean";
            if (args.Length != 4)
                throw new ArgumentException($"{signature} requires 4 arguments");
            
            if (args[0].IsString() == false)
                throw new ArgumentException($"{signature}: First argument must be a hash algorithm string ('SHA-256', 'SHA-384', or 'SHA-512')");
            
            string hashName = args[0].AsString();
            byte[] keyData = GetKeyData(args[1], signature);
            
            // Handle signature - can be base64 string or buffer
            byte[] signatureBytes;
            if (args[2].IsString())
            {
                try
                {
                    signatureBytes = Convert.FromBase64String(args[2].AsString());
                }
                catch
                {
                    throw new ArgumentException($"{signature}: Signature must be a valid base64 string or BufferSource");
                }
            }
            else
            {
                signatureBytes = GetBytesFromJsValue(args[2], signature);
            }
            
            byte[] data = GetBytesFromJsValue(args[3], signature);

            var computed = hashName switch
            {
                "SHA-256" => HMACSHA256.HashData(keyData, data),
                "SHA-384" => HMACSHA384.HashData(keyData, data),
                "SHA-512" => HMACSHA512.HashData(keyData, data),
                _ => throw new NotSupportedException($"{signature}: Unsupported hash algorithm '{hashName}'")
            };

            bool valid = CryptographicOperations.FixedTimeEquals(computed, signatureBytes);
            return valid ? JsBoolean.True : JsBoolean.False;
        }

        private JsValue Crypto_EncryptAesGcm(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.encryptAesGcm(iv: BufferSource | string (base64), key: BufferSource | string (base64), data: BufferSource | string): string (base64)";
            if (args.Length != 3)
                throw new ArgumentException($"{signature} requires 3 arguments");

            byte[] iv = GetBytesFromBase64OrBuffer(args[0], signature, "iv");
            byte[] key = GetBytesFromBase64OrBuffer(args[1], signature, "key");
            byte[] plain = GetBytesFromJsValue(args[2], signature);

            const int tagSizeInBytes = 16;
            var result = new byte[plain.Length + tagSizeInBytes];
            Span<byte> buffer = result;

            using (var aes = new AesGcm(key, tagSizeInBytes))
            {
                aes.Encrypt(iv, plain, buffer.Slice(0, buffer.Length - tagSizeInBytes), buffer.Slice(buffer.Length - tagSizeInBytes));
            }

            return Convert.ToBase64String(result);
        }

        private JsValue Crypto_DecryptAesGcm(JsValue self, JsValue[] args)
        {
            const string signature = "crypto.decryptAesGcm(iv: BufferSource | string (base64), key: BufferSource | string (base64), data: string (base64) | BufferSource, outputType: 'string' | 'raw'): string | ArrayBuffer";
            if (args.Length < 3 || args.Length > 4)
                throw new ArgumentException($"{signature} requires 3 or 4 arguments");

            byte[] iv = GetBytesFromBase64OrBuffer(args[0], signature, "iv");
            byte[] key = GetBytesFromBase64OrBuffer(args[1], signature, "key");
            
            // Handle encrypted data - can be base64 string or buffer
            byte[] cipherWithTag;
            if (args[2].IsString())
            {
                try
                {
                    cipherWithTag = Convert.FromBase64String(args[2].AsString());
                }
                catch
                {
                    throw new ArgumentException($"{signature}: Encrypted data must be a valid base64 string or BufferSource");
                }
            }
            else
            {
                cipherWithTag = GetBytesFromJsValue(args[2], signature);
            }

            const int tagSizeInBytes = 16;
            if (cipherWithTag.Length < tagSizeInBytes)
                throw new ArgumentException($"{signature}: Ciphertext too short/invalid");

            var plain = new byte[cipherWithTag.Length - tagSizeInBytes];

            using (var aes = new AesGcm(key, tagSizeInBytes))
            {
                ReadOnlySpan<byte> input = cipherWithTag;
                aes.Decrypt(iv, input.Slice(0, input.Length - tagSizeInBytes), input.Slice(input.Length - tagSizeInBytes), plain);
            }

            string outputType = "string"; 
            if (args.Length == 4)
            {
                if (args[3].IsString() == false)
                    throw new ArgumentException($"{signature}: outputType must be 'string' or 'raw'");
                outputType = args[3].AsString();
            }

            return outputType switch
            {
                "string" => Encoding.UTF8.GetString(plain),
                "raw" => CreateArrayBuffer(plain),
                _ => throw new ArgumentException($"{signature}: outputType must be 'string' or 'raw', got '{outputType}'")
            };
        }

        private string GetAlgorithmName(JsValue arg, string signature)
        {
            if (arg.IsString())
                return arg.AsString();
            if (arg.IsObject() && arg.AsObject().TryGetValue("name", out var name))
                return name.AsString();
            throw new ArgumentException($"{signature}: Invalid algorithm argument");
        }

        private byte[] GetBytesFromBase64OrBuffer(JsValue val, string signature, string paramName)
        {
            if (val.IsString())
            {
                try
                {
                    return Convert.FromBase64String(val.AsString());
                }
                catch
                {
                    throw new ArgumentException($"{signature}: {paramName} must be a valid base64 string or BufferSource");
                }
            }
            return GetBytesFromJsValue(val, signature);
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
