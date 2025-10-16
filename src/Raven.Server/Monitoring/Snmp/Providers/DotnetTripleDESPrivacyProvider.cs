// DES privacy provider.
// Copyright (C) 2008-2010 Malcolm Crowe, Lex Li, and other contributors.
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of this
// software and associated documentation files (the "Software"), to deal in the Software
// without restriction, including without limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
// to whom the Software is furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all copies or
// substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
// INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
// PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
// FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Security;

namespace Raven.Server.Monitoring.Snmp.Providers;

/// <summary>
/// Privacy provider for 3DES.
/// </summary>
/// <remarks>
/// <p>Ported from SNMP#NET Privacy3DES class.</p>
/// <p>Originally defined in a draft https://datatracker.ietf.org/doc/html/draft-reeder-snmpv3-usm-3desede-00</p>
/// </remarks>
[Obsolete("3DES is no longer secure. Please use a more secure provider.")]
public sealed class DotnetTripleDESPrivacyProvider : IPrivacyProvider
{
#if NET6_0_OR_GREATER
    /// <summary>
    /// Flag to force using legacy encryption/decryption code on .NET 6.
    /// </summary>
    public static bool UseLegacy { get; set; }
#endif
    private readonly SaltGenerator _salt = new();
    private readonly OctetString _phrase;

    /// <summary>
    /// Initializes a new instance of the <see cref="phrase"/> class.
    /// </summary>
    /// <param name="auth">The phrase.</param>
    /// <param name="auth">The authentication provider.</param>
    public DotnetTripleDESPrivacyProvider(OctetString phrase, IAuthenticationProvider auth)
    {
        if (auth == null)
        {
            throw new ArgumentNullException(nameof(auth));
        }

        // IMPORTANT: in this way privacy cannot be non-default.
        if (auth == DefaultAuthenticationProvider.Instance)
        {
            throw new ArgumentException("If authentication is off, then privacy cannot be used.", nameof(auth));
        }

        _phrase = phrase ?? throw new ArgumentNullException(nameof(phrase));
        AuthenticationProvider = auth;
    }

    /// <summary>
    /// Corresponding <see cref="IAuthenticationProvider"/>.
    /// </summary>
    public IAuthenticationProvider AuthenticationProvider { get; }

    public OctetString EngineId { get; }

    /// <summary>
    /// Engine IDs.
    /// </summary>
    /// <remarks>This is an optional field, and only used by TRAP v2 authentication.</remarks>
    public ICollection<OctetString>? EngineIds { get; set; }

    /// <summary>
    /// Encrypt scoped PDU using DES encryption protocol
    /// </summary>
    /// <param name="unencryptedData">Unencrypted scoped PDU byte array</param>
    /// <param name="key">Encryption key. Key has to be at least 32 bytes is length</param>
    /// <param name="privacyParameters">Privacy parameters out buffer. This field will be filled in with information
    /// required to decrypt the information. Output length of this field is 8 bytes and space has to be reserved
    /// in the USM header to store this information</param>
    /// <returns>Encrypted byte array</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when encryption key is null or length of the encryption key is too short.</exception>
    public static byte[] Encrypt(byte[] unencryptedData, byte[] key, byte[] privacyParameters)
    {
        if (unencryptedData == null)
        {
            throw new ArgumentNullException(nameof(unencryptedData));
        }

        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (privacyParameters == null)
        {
            throw new ArgumentNullException(nameof(privacyParameters));
        }

        if (key.Length < MinimumKeyLength)
        {
            throw new ArgumentException($"Encryption key length has to 32 bytes or more. Current: {key.Length}.", nameof(key));
        }

        var iv = GetIV(key, privacyParameters);
        var outKey = GetKey(key);

        // IMPORTANT: "The data to be encrypted is treated as sequence of octets. Its
        // length should be an integral multiple of 8 - and if it is not, the
        // data is padded at the end as necessary. The actual pad value is
        // irrelevant."
        if ((unencryptedData.Length % 8) != 0)
        {
            byte[] tmpBuffer = new byte[8 * ((unencryptedData.Length / 8) + 1)];
            Buffer.BlockCopy(unencryptedData, 0, tmpBuffer, 0, unencryptedData.Length);
            unencryptedData = tmpBuffer;
        }
#if NET6_0_OR_GREATER
        return UseLegacy ? LegacyEncrypt(outKey, iv, unencryptedData) : Net6Encrypt(outKey, iv, unencryptedData);
#else
            return LegacyEncrypt(outKey, iv, unencryptedData);
#endif
    }

#if NET6_0_OR_GREATER
    internal static byte[] Net6Encrypt(byte[] key, byte[] iv, byte[] unencryptedData)
    {
        using TripleDES des = TripleDES.Create();
        des.Key = key;
        return des.EncryptCbc(unencryptedData, iv, PaddingMode.None);
    }
#endif

    internal static byte[] LegacyEncrypt(byte[] key, byte[] iv, byte[] unencryptedData)
    {
        using TripleDES des = TripleDES.Create();
        des.Mode = CipherMode.CBC;
        des.Padding = PaddingMode.None;

        using var transform = des.CreateEncryptor(key, iv);
        return transform.TransformFinalBlock(unencryptedData, 0, unencryptedData.Length);
    }

    /// <summary>
    /// Decrypt DES encrypted scoped PDU.
    /// </summary>
    /// <param name="encryptedData">Source data buffer</param>
    /// <param name="key">Decryption key. Key length has to be 32 bytes in length or longer (bytes beyond 32 bytes are ignored).</param>
    /// <param name="privacyParameters">Privacy parameters extracted from USM header</param>
    /// <returns>Decrypted byte array</returns>
    /// <exception cref="ArgumentNullException">Thrown when encrypted data is null or length == 0</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when encryption key length is less then 32 byte or if privacy parameters
    /// argument is null or length other then 8 bytes</exception>
    public static byte[] Decrypt(byte[] encryptedData, byte[] key, byte[] privacyParameters)
    {
        if (encryptedData == null)
        {
            throw new ArgumentNullException(nameof(encryptedData));
        }

        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (privacyParameters == null)
        {
            throw new ArgumentNullException(nameof(privacyParameters));
        }

        if (encryptedData.Length == 0)
        {
            throw new ArgumentException("Empty encrypted data.", nameof(encryptedData));
        }

        if ((encryptedData.Length % 8) != 0)
        {
            throw new ArgumentException("Encrypted data buffer has to be divisible by 8.", nameof(encryptedData));
        }

        if (privacyParameters.Length != PrivacyParametersLength)
        {
            throw new ArgumentOutOfRangeException(nameof(privacyParameters), "Privacy parameters argument has to be 8 bytes long.");
        }

        if (key.Length < MinimumKeyLength)
        {
            throw new ArgumentOutOfRangeException(nameof(key), "Decryption key has to be at least 16 bytes long.");
        }

        var iv = GetIV(key, privacyParameters);
        var outKey = GetKey(key);

#if NET6_0_OR_GREATER
        return UseLegacy ? LegacyDecrypt(outKey, iv, encryptedData) : Net6Decrypt(outKey, iv, encryptedData);
#else
            return LegacyDecrypt(outKey, iv, encryptedData);
#endif
    }

#if NET6_0_OR_GREATER
    internal static byte[] Net6Decrypt(byte[] key, byte[] iv, byte[] encryptedData)
    {
        using TripleDES des = TripleDES.Create();
        des.Key = key;
        return des.DecryptCbc(encryptedData, iv, PaddingMode.Zeros);
    }
#endif

    internal static byte[] LegacyDecrypt(byte[] key, byte[] iv, byte[] encryptedData)
    {
        using TripleDES des = TripleDES.Create();
        des.Mode = CipherMode.CBC;
        des.Padding = PaddingMode.Zeros;

        using var transform = des.CreateDecryptor(key, iv);
        var decryptedData = transform.TransformFinalBlock(encryptedData, 0, encryptedData.Length);
        return decryptedData;
    }

    /// <summary>
    /// Generate IV from the privacy key and salt value returned by GetSalt method.
    /// </summary>
    /// <param name="privacyKey">32 byte privacy key</param>
    /// <param name="salt">Salt value returned by GetSalt method</param>
    /// <returns>IV value used in the encryption process</returns>
    private static byte[] GetIV(IList<byte> privacyKey, IList<byte> salt)
    {
        if (privacyKey.Count < MinimumKeyLength)
        {
            throw new ArgumentException("Invalid privacy key length", nameof(privacyKey));
        }

        var iv = new byte[8];
        for (var i = 0; i < iv.Length; i++)
        {
            iv[i] = (byte)(salt[i] ^ privacyKey[24 + i]);
        }

        return iv;
    }

    /// <summary>
    /// Extract and return DES encryption key.
    /// Privacy password is 16 bytes in length. Only the first 8 bytes are used as DES password. Remaining
    /// 8 bytes are used as pre-IV value.
    /// </summary>
    /// <param name="privacyPassword">16 byte privacy password</param>
    /// <returns>8 byte DES encryption password</returns>
    private static byte[] GetKey(byte[] privacyPassword)
    {
        if (privacyPassword.Length < 32)
        {
            throw new ArgumentException("Invalid privacy key length.", nameof(privacyPassword));
        }

        // Important: "The first 24 octets of the 32-octet secret (private privacy key) are used as a 3DES-EDE key."
        var key = new byte[24];
        Buffer.BlockCopy(privacyPassword, 0, key, 0, key.Length);

        // TODO: verify that K1, K2, K3 are unique.
        return key;
    }

    /// <summary>
    /// Returns the length of privacyParameters USM header field. For 3DES, field length is 8.
    /// </summary>
    public static int PrivacyParametersLength => 8;

    /// <summary>
    /// Returns minimum encryption/decryption key length. For 3DES, returned value is 32.
    /// 
    /// 3DES protocol itself requires an 24 byte key. Additional 8 bytes are used for generating the
    /// encryption IV. For encryption itself, first 24 bytes of the key are used.
    /// </summary>
    public static int MinimumKeyLength => MaximumKeyLength;

    /// <summary>
    /// Return maximum encryption/decryption key length. For 3DES, returned value is 32.
    /// 
    /// 3DES protocol itself requires an 24 byte key. Additional 8 bytes are used for generating the
    /// encryption IV. For encryption itself, first 24 bytes of the key are used.
    /// </summary>
    public static int MaximumKeyLength => 32;

    #region IPrivacyProvider Members

    /// <summary>
    /// Decrypts the specified data.
    /// </summary>
    /// <param name="data">The data.</param>
    /// <param name="parameters">The parameters.</param>
    /// <returns></returns>
    public ISnmpData Decrypt(ISnmpData data, SecurityParameters parameters)
    {
        if (data == null)
        {
            throw new ArgumentNullException(nameof(data));
        }

        if (parameters == null)
        {
            throw new ArgumentNullException(nameof(parameters));
        }

        var code = data.TypeCode;
        if (code != SnmpType.OctetString)
        {
            throw new ArgumentException($"Cannot decrypt the scope data: {code}.", nameof(data));
        }

        if (parameters.EngineId == null)
        {
            throw new ArgumentException("Invalid security parameters", nameof(parameters));
        }

        var octets = (OctetString)data;
        var bytes = octets.GetRaw();
        var pkey = PasswordToKey(_phrase.GetRaw(), parameters.EngineId.GetRaw());

        try
        {
            // decode encrypted packet
            var decrypted = Decrypt(bytes, pkey, parameters.PrivacyParameters!.GetRaw());
            var result = DataFactory.CreateSnmpData(decrypted);
            if (result.TypeCode != SnmpType.Sequence)
            {
                var newException = new DecryptionException("DES decryption failed");
                newException.SetBytes(bytes);
                throw newException;
            }

            return result;
        }
        catch (Exception ex)
        {
            var newException = new DecryptionException("DES decryption failed", ex);
            newException.SetBytes(bytes);
            throw newException;
        }
    }

    /// <summary>
    /// Encrypts the specified scope.
    /// </summary>
    /// <param name="data">The scope data.</param>
    /// <param name="parameters">The parameters.</param>
    /// <returns></returns>
    public ISnmpData Encrypt(ISnmpData data, SecurityParameters parameters)
    {
        if (data == null)
        {
            throw new ArgumentNullException(nameof(data));
        }

        if (parameters == null)
        {
            throw new ArgumentNullException(nameof(parameters));
        }

        if (data.TypeCode != SnmpType.Sequence && data is not ISnmpPdu)
        {
            throw new ArgumentException("Invalid data type.", nameof(data));
        }

        if (parameters.EngineId == null)
        {
            throw new ArgumentException("Invalid security parameters", nameof(parameters));
        }

        var pkey = PasswordToKey(_phrase.GetRaw(), parameters.EngineId.GetRaw());
        var bytes = data.ToBytes();
        var reminder = bytes.Length % 8;
        var count = reminder == 0 ? 0 : 8 - reminder;
        using (var stream = new MemoryStream())
        {
            stream.Write(bytes, 0, bytes.Length);
            for (var i = 0; i < count; i++)
            {
                stream.WriteByte(1);
            }

            bytes = stream.ToArray();
        }

        var encrypted = Encrypt(bytes, pkey, parameters.PrivacyParameters!.GetRaw());
        return new OctetString(encrypted);
    }

    /// <summary>
    /// Gets the salt.
    /// </summary>
    /// <value>The salt.</value>
    public OctetString Salt => new(_salt.GetSaltBytes());

    /// <summary>
    /// Passwords to key.
    /// </summary>
    /// <param name="secret">The secret.</param>
    /// <param name="engineId">The engine identifier.</param>
    /// <returns></returns>
    public byte[] PasswordToKey(byte[] secret, byte[] engineId)
    {
        byte[] encryptionKey = AuthenticationProvider.PasswordToKey(secret, engineId);
        if (encryptionKey.Length < MinimumKeyLength)
        {
            encryptionKey = ExtendShortKey(encryptionKey, engineId, AuthenticationProvider);
        }

        return encryptionKey;
    }

    internal static byte[] ExtendShortKey(byte[] shortKey, byte[] engineID, IAuthenticationProvider authProtocol)
    {
        int length = shortKey.Length;
        byte[] extendedKey = new byte[MinimumKeyLength];
        Buffer.BlockCopy(shortKey, 0, extendedKey, 0, shortKey.Length);

        while (length < MinimumKeyLength)
        {
            byte[] key =
                authProtocol.PasswordToKey(shortKey,
                    engineID);
            int copyBytes = Math.Min(MaximumKeyLength - length,
                authProtocol.DigestLength);
            Buffer.BlockCopy(key, 0, extendedKey, length, copyBytes);
            length += copyBytes;
        }

        return extendedKey;
    }

    #endregion

    /// <summary>
    /// Returns a string that represents this object.
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        return "3DES privacy provider";
    }
}
