// -----------------------------------------------------------------------
//  <copyright file="FipsEncryptor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Security.Cryptography;

namespace Raven.Client.Util.Encryption
{
    internal sealed class FipsEncryptor : EncryptorBase<FipsEncryptor.FipsHashEncryptor, FipsEncryptor.FipsSymmetricalEncryptor, FipsEncryptor.FipsAsymmetricalEncryptor>
    {
        public FipsEncryptor()
        {
            Hash = new FipsHashEncryptor(allowNonThreadSafeMethods: false);
        }

        public override IHashEncryptor Hash { get; protected set; }

        internal class FipsHashEncryptor : HashEncryptorBase, IHashEncryptor
        {
            public FipsHashEncryptor()
                : this(true)
            {
            }

            public FipsHashEncryptor(bool allowNonThreadSafeMethods)
                : base(allowNonThreadSafeMethods)
            {
            }

            public void Dispose()
            {
                //no-op
            }

            ABCDStruct _abcdStruct = MD5Core.GetInitialStruct();
            private readonly byte[] _remainingBuffer = new byte[BufferSize];
            private int _remainingCount;
            private int _totalLength;

            private const int BufferSize = 64;

            public byte[] Compute16(Stream stream)
            {
                ThrowNotSupportedExceptionForNonThreadSafeMethod();

                var buffer = new byte[4096];
                int bytesRead;
                do
                {
                    bytesRead = stream.Read(buffer, 0, 4096);
                    if (bytesRead > 0)
                    {
                        TransformBlock(buffer, 0, bytesRead);
                    }
                } while (bytesRead > 0);

                return TransformFinalBlock();
            }

            public byte[] TransformFinalBlock()
            {
                ThrowNotSupportedExceptionForNonThreadSafeMethod();

                _totalLength += _remainingCount;
                return MD5Core.GetHashFinalBlock(_remainingBuffer, 0, _remainingCount, _abcdStruct, (Int64)_totalLength * 8);
            }

            public void TransformBlock(byte[] bytes, int offset, int length)
            {
                ThrowNotSupportedExceptionForNonThreadSafeMethod();

                int start = offset;
                if (_remainingCount > 0)
                {
                    if (_remainingCount + length < BufferSize)
                    {
                        // just append to remaining buffer
                        Buffer.BlockCopy(bytes, offset, _remainingBuffer, _remainingCount, length);
                        _remainingCount += length;
                        return;
                    }

                    // fill up buffer
                    Buffer.BlockCopy(bytes, offset, _remainingBuffer, _remainingCount, BufferSize - _remainingCount);
                    start += BufferSize - _remainingCount;
                    // now we have 64 bytes in buffer
                    MD5Core.GetHashBlock(_remainingBuffer, ref _abcdStruct, 0);
                    _totalLength += BufferSize;
                    _remainingCount = 0;
                }

                // while has 64 bytes blocks
                while (start <= length - BufferSize)
                {
                    MD5Core.GetHashBlock(bytes, ref _abcdStruct, start);
                    _totalLength += BufferSize;
                    start += BufferSize;
                }

                // save rest (if any)
                if (start != length)
                {
                    _remainingCount = length - start;
                    Buffer.BlockCopy(bytes, start, _remainingBuffer, 0, _remainingCount);
                }

            }

            public int StorageHashSize => 20;

            //SHA1
            public byte[] ComputeForStorage(byte[] bytes)
            {
                if (StorageHashSize == 20)
                    return Compute20(bytes);
                return Compute16(bytes);
            }

            public byte[] ComputeForStorage(byte[] bytes, int offset, int length)
            {
                if (StorageHashSize == 20)
                    return Compute20(bytes, offset, length);
                return Compute16(bytes, offset, length);
            }

            public byte[] ComputeForOAuth(byte[] bytes)
            {
                return Compute20(bytes);
            }

            public byte[] Compute16(byte[] bytes)
            {
                return MD5Core.GetHash(bytes);
            }

            public byte[] Compute16(byte[] bytes, int offset, int length)
            {
                return MD5Core.GetHash(bytes, offset, length);
            }

            public byte[] Compute20(byte[] bytes)
            {
                return ComputeHash(SHA1.Create(), bytes, 20);
            }

            public byte[] Compute20(byte[] bytes, int offset, int length)
            {
                return ComputeHash(SHA1.Create(), bytes, offset, length, 20);
            }
        }

        internal class FipsSymmetricalEncryptor : ISymmetricalEncryptor
        {
            private readonly SymmetricAlgorithm _algorithm;

            public FipsSymmetricalEncryptor()
            {
                _algorithm = Aes.Create();
            }

            public byte[] Key
            {
                get => _algorithm.Key;

                set => _algorithm.Key = value;
            }

            public byte[] IV
            {
                get => _algorithm.IV;

                set => _algorithm.IV = value;
            }

            public int KeySize
            {
                get => _algorithm.KeySize;

                set => _algorithm.KeySize = value;
            }

            public void GenerateKey()
            {
                _algorithm.GenerateKey();
            }

            public void GenerateIV()
            {
                _algorithm.GenerateIV();
            }

            public ICryptoTransform CreateEncryptor()
            {
                return _algorithm.CreateEncryptor();
            }

            public ICryptoTransform CreateDecryptor()
            {
                return _algorithm.CreateDecryptor();
            }

            public ICryptoTransform CreateDecryptor(byte[] key, byte[] iv)
            {
                return _algorithm.CreateDecryptor(key, iv);
            }

            public void Dispose()
            {
                _algorithm?.Dispose();
            }
        }

        internal class FipsAsymmetricalEncryptor : IAsymmetricalEncryptor
        {
            private readonly RSACng _algorithm;

            public FipsAsymmetricalEncryptor()
            {

                _algorithm = new RSACng();
            }

            public FipsAsymmetricalEncryptor(int keySize)
            {
                _algorithm = new RSACng(keySize);
            }

            public int KeySize
            {
                get => _algorithm.KeySize;

                set => _algorithm.KeySize = value;
            }

            public AsymmetricAlgorithm Algorithm => _algorithm;

            public void ImportParameters(byte[] exponent, byte[] modulus)
            {
                _algorithm.ImportParameters(new RSAParameters
                {
                    Modulus = modulus,
                    Exponent = exponent
                });
            }

            public byte[] Encrypt(byte[] bytes)
            {
                return _algorithm.Encrypt(bytes, RSAEncryptionPadding.OaepSHA256);
            }

            public byte[] Decrypt(byte[] bytes)
            {
                return _algorithm.Decrypt(bytes, RSAEncryptionPadding.OaepSHA256);
            }

            public void ImportParameters(RSAParameters parameters)
            {
                _algorithm.ImportParameters(parameters);
            }

            public RSAParameters ExportParameters(bool includePrivateParameters)
            {
                return _algorithm.ExportParameters(includePrivateParameters);
            }

            public byte[] SignHash(byte[] data)
            {
                return _algorithm.SignHash(data, HashAlgorithmName.SHA1, RSASignaturePadding.Pkcs1);
            }

            public bool VerifyHash(byte[] hash, byte[] signature)
            {
                return _algorithm.VerifyHash(hash, signature, HashAlgorithmName.SHA1, RSASignaturePadding.Pkcs1);
            }

            public void Dispose()
            {
                _algorithm?.Dispose();
            }
        }
    }

}
