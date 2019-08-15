// Bouncy Castle AES privacy provider
// Copyright (C) 2009-2010 Lex Li, Milan Sinadinovic
// Copyright (C) 2018 Matt Zinkevicius
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
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Security;
using Org.BouncyCastle.Crypto.Engines;
using Org.BouncyCastle.Crypto.Modes;
using Org.BouncyCastle.Crypto.Paddings;
using Org.BouncyCastle.Crypto.Parameters;

namespace Raven.Server.Monitoring.Snmp.Providers
{
    /// <summary>
    /// Privacy provider base for AES.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "AES", Justification = "definition")]
    public abstract class BouncyCastleAESPrivacyProviderBase : IPrivacyProvider
    {
        private readonly SaltGenerator _salt = new SaltGenerator();
        private readonly OctetString _phrase;

        /// <summary>
        /// Initializes a new instance of the <see cref="BouncyCastleAESPrivacyProviderBase"/> class.
        /// </summary>
        /// <param name="keyBytes">Key bytes.</param>
        /// <param name="phrase">The phrase.</param>
        /// <param name="auth">The authentication provider.</param>
        protected BouncyCastleAESPrivacyProviderBase(int keyBytes, OctetString phrase, IAuthenticationProvider auth)
        {
            if (keyBytes != 16 && keyBytes != 24 && keyBytes != 32)
            {
                throw new ArgumentOutOfRangeException(nameof(keyBytes), "Valid key sizes are 16, 24 and 32 bytes.");
            }

            if (auth == null)
            {
                throw new ArgumentNullException(nameof(auth));
            }

            KeyBytes = keyBytes;

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

        [Obsolete("Use EngineIds instead.")]
        public OctetString EngineId { get; set; }

        /// <summary>
        /// Engine IDs.
        /// </summary>
        /// <remarks>This is an optional field, and only used by TRAP v2 authentication.</remarks>
        public ICollection<OctetString> EngineIds { get; set; }

        /// <summary>
        /// Encrypt scoped PDU
        /// </summary>
        /// <param name="unencryptedData">Unencrypted scoped PDU byte array</param>
        /// <param name="key">Encryption key. Key has to be at least 32 bytes is length</param>
        /// <param name="engineBoots">Engine boots.</param>
        /// <param name="engineTime">Engine time.</param>
        /// <param name="privacyParameters">Privacy parameters out buffer. This field will be filled in with information
        /// required to decrypt the information. Output length of this field is 8 bytes and space has to be reserved
        /// in the USM header to store this information</param>
        /// <returns>Encrypted byte array</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when encryption key is null or length of the encryption key is too short.</exception>
        internal byte[] Encrypt(byte[] unencryptedData, byte[] key, int engineBoots, int engineTime, byte[] privacyParameters)
        {
            // check the key before doing anything else
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length < KeyBytes)
            {
                throw new ArgumentOutOfRangeException(nameof(key), "Invalid key length.");
            }

            if (unencryptedData == null)
            {
                throw new ArgumentNullException(nameof(unencryptedData));
            }

            // Set privacy parameters to the local 64 bit salt value
            var iv = new byte[16];
            var bootsBytes = BitConverter.GetBytes(engineBoots);
            iv[0] = bootsBytes[3];
            iv[1] = bootsBytes[2];
            iv[2] = bootsBytes[1];
            iv[3] = bootsBytes[0];
            var timeBytes = BitConverter.GetBytes(engineTime);
            iv[4] = timeBytes[3];
            iv[5] = timeBytes[2];
            iv[6] = timeBytes[1];
            iv[7] = timeBytes[0];

            // Copy salt value to the iv array
            Buffer.BlockCopy(privacyParameters, 0, iv, 8, PrivacyParametersLength);

            // Resize the key, if necessary, to the required length
            Array.Resize(ref key, KeyBytes);

            // Encrypt using BouncyCastle
            var blockCipher = new CfbBlockCipher(new AesEngine(), 128);
            var paddedCipher = new PaddedBufferedBlockCipher(blockCipher, new ZeroBytePadding());
            var cipherParameters = new ParametersWithIV(new KeyParameter(key), iv);
            paddedCipher.Init(true, cipherParameters);
            byte[] encryptedData = paddedCipher.DoFinal(unencryptedData);

            // Trim off padding, if necessary
            Array.Resize(ref encryptedData, unencryptedData.Length);

            return encryptedData;
        }

        /// <summary>
        /// Decrypt scoped PDU.
        /// </summary>
        /// <param name="encryptedData">Source data buffer</param>
        /// <param name="engineBoots">Engine boots.</param>
        /// <param name="engineTime">Engine time.</param>
        /// <param name="key">Decryption key. Key length has to be 32 bytes in length or longer (bytes beyond 32 bytes are ignored).</param>
        /// <param name="privacyParameters">Privacy parameters extracted from USM header</param>
        /// <returns>Decrypted byte array</returns>
        /// <exception cref="ArgumentNullException">Thrown when encrypted data is null or length == 0</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when encryption key length is less then 32 byte or if privacy parameters
        /// argument is null or length other then 8 bytes</exception>
        internal byte[] Decrypt(byte[] encryptedData, byte[] key, int engineBoots, int engineTime, byte[] privacyParameters)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (encryptedData == null)
            {
                throw new ArgumentNullException(nameof(encryptedData));
            }

            if (key.Length < KeyBytes)
            {
                throw new ArgumentOutOfRangeException(nameof(key), "Invalid key length.");
            }

            // Set privacy parameters to the local 64 bit salt value
            var iv = new byte[16];
            var bootsBytes = BitConverter.GetBytes(engineBoots);
            iv[0] = bootsBytes[3];
            iv[1] = bootsBytes[2];
            iv[2] = bootsBytes[1];
            iv[3] = bootsBytes[0];
            var timeBytes = BitConverter.GetBytes(engineTime);
            iv[4] = timeBytes[3];
            iv[5] = timeBytes[2];
            iv[6] = timeBytes[1];
            iv[7] = timeBytes[0];

            // Copy salt value to the iv array
            Buffer.BlockCopy(privacyParameters, 0, iv, 8, PrivacyParametersLength);

            // Resize the key, if necessary, to the required length
            Array.Resize(ref key, KeyBytes);

            // Pad encrypted data to a multiple of 16 bytes
            byte[] encryptedDataPadded = encryptedData;
            if (encryptedData.Length % KeyBytes != 0)
            {
                var div = (int)Math.Floor(encryptedData.Length / (double)16);
                var newLength = (div + 1) * 16;
                encryptedDataPadded = new byte[newLength];
                Buffer.BlockCopy(encryptedData, 0, encryptedDataPadded, 0, encryptedData.Length);
            }

            // Decrypt using BouncyCastle
            var blockCipher = new CfbBlockCipher(new AesEngine(), 128);
            var paddedCipher = new PaddedBufferedBlockCipher(blockCipher, new ZeroBytePadding());
            var cipherParameters = new ParametersWithIV(new KeyParameter(key), iv);
            paddedCipher.Init(false, cipherParameters);
            var decryptedData = paddedCipher.DoFinal(encryptedDataPadded);

            // Trim off padding, if necessary
            Array.Resize(ref decryptedData, encryptedData.Length);

            return decryptedData;
        }

        /// <summary>
        /// Returns the length of privacyParameters USM header field. For AES, field length is 8.
        /// </summary>
        private static int PrivacyParametersLength => 8;

        /// <summary>
        /// Returns minimum encryption/decryption key length. For DES, returned value is 16.
        /// 
        /// DES protocol itself requires an 8 byte key. Additional 8 bytes are used for generating the
        /// encryption IV. For encryption itself, first 8 bytes of the key are used.
        /// </summary>
        private int MinimumKeyLength => KeyBytes;

        /// <summary>
        /// Return maximum encryption/decryption key length. For DES, returned value is 16
        /// 
        /// DES protocol itself requires an 8 byte key. Additional 8 bytes are used for generating the
        /// encryption IV. For encryption itself, first 8 bytes of the key are used.
        /// </summary>
        public int MaximumKeyLength => KeyBytes;

        #region IPrivacyProvider Members

        /// <summary>
        /// Decrypts the specified data.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public ISnmpData Decrypt(ISnmpData data, SecurityParameters parameters)
        {
            if (parameters == null)
            {
                throw new ArgumentNullException(nameof(parameters));
            }

            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            var code = data.TypeCode;
            if (code != SnmpType.OctetString)
            {
                throw new ArgumentException($"Cannot decrypt the scope data: {code}.", nameof(data));
            }

            var octets = (OctetString)data;
            var bytes = octets.GetRaw();
            var pkey = PasswordToKey(_phrase.GetRaw(), parameters.EngineId.GetRaw());

            // decode encrypted packet
            var decrypted = Decrypt(bytes, pkey, parameters.EngineBoots.ToInt32(), parameters.EngineTime.ToInt32(), parameters.PrivacyParameters.GetRaw());
            return DataFactory.CreateSnmpData(decrypted);
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

            if (data.TypeCode != SnmpType.Sequence && !(data is ISnmpPdu))
            {
                throw new ArgumentException("Invalid data type.", nameof(data));
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

            var encrypted = Encrypt(bytes, pkey, parameters.EngineBoots.ToInt32(), parameters.EngineTime.ToInt32(), parameters.PrivacyParameters.GetRaw());
            return new OctetString(encrypted);
        }

        /// <summary>
        /// Gets the salt.
        /// </summary>
        /// <value>The salt.</value>
        public OctetString Salt => new OctetString(_salt.GetSaltBytes());

        public int KeyBytes { get; }

        public byte[] PasswordToKey(byte[] secret, byte[] engineId)
        {
            var pkey = AuthenticationProvider.PasswordToKey(secret, engineId);
            if (pkey.Length < MinimumKeyLength)
            {
                pkey = ExtendShortKey(pkey, secret, engineId, AuthenticationProvider);
            }

            return pkey;
        }

        #endregion

        /// <summary>
        /// Some protocols support a method to extend the encryption or decryption key when supplied key
        /// is too short.
        /// </summary>
        /// <param name="shortKey">Key that needs to be extended</param>
        /// <param name="password">Privacy password as configured on the SNMP agent.</param>
        /// <param name="engineID">Authoritative engine id. Value is retrieved as part of SNMP v3 discovery procedure</param>
        /// <param name="authProtocol">Authentication protocol class instance cast as <see cref="IAuthenticationProvider"/></param>
        /// <returns>Extended key value</returns>
        public byte[] ExtendShortKey(byte[] shortKey, byte[] password, byte[] engineID, IAuthenticationProvider authProtocol)
        {
            byte[] extKey = new byte[MinimumKeyLength];
            byte[] lastKeyBuf = new byte[shortKey.Length];
            Array.Copy(shortKey, lastKeyBuf, shortKey.Length);
            int keyLen = shortKey.Length > MinimumKeyLength ? MinimumKeyLength : shortKey.Length;
            Array.Copy(shortKey, extKey, keyLen);
            while (keyLen < MinimumKeyLength)
            {
                byte[] tmpBuf = authProtocol.PasswordToKey(lastKeyBuf, engineID);
                if (tmpBuf == null)
                {
                    return null;
                }

                if (tmpBuf.Length <= (MinimumKeyLength - keyLen))
                {
                    Array.Copy(tmpBuf, 0, extKey, keyLen, tmpBuf.Length);
                    keyLen += tmpBuf.Length;
                }
                else
                {
                    Array.Copy(tmpBuf, 0, extKey, keyLen, MinimumKeyLength - keyLen);
                    keyLen += (MinimumKeyLength - keyLen);
                }

                lastKeyBuf = new byte[tmpBuf.Length];
                Array.Copy(tmpBuf, lastKeyBuf, tmpBuf.Length);
            }

            return extKey;
        }
    }
}
