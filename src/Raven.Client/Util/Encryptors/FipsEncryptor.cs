// -----------------------------------------------------------------------
//  <copyright file="FipsEncryptor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Security.Cryptography;

namespace Raven.NewClient.Abstractions.Util.Encryptors
{
    public sealed class FipsEncryptor : EncryptorBase<FipsEncryptor.FipsHashEncryptor, FipsEncryptor.FipsSymmetricalEncryptor, FipsEncryptor.FipsAsymmetricalEncryptor>
    {
        public FipsEncryptor()
        {
            Hash = new FipsHashEncryptor(allowNonThreadSafeMethods: false);
        }

        public override IHashEncryptor Hash { get; protected set; }

        public class FipsHashEncryptor : HashEncryptorBase, IHashEncryptor
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

            ABCDStruct abcdStruct = MD5Core.GetInitialStruct();
            private readonly byte[] remainingBuffer = new byte[BufferSize];
            private int remainingCount;
            private int totalLength;

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

                totalLength += remainingCount;
                return MD5Core.GetHashFinalBlock(remainingBuffer, 0, remainingCount, abcdStruct, (Int64)totalLength * 8);
            }

            public void TransformBlock(byte[] bytes, int offset, int length)
            {
                ThrowNotSupportedExceptionForNonThreadSafeMethod();

                int start = offset;
                if (remainingCount > 0)
                {
                    if (remainingCount + length < BufferSize)
                    {
                        // just append to remaining buffer
                        Buffer.BlockCopy(bytes, offset, remainingBuffer, remainingCount, length);
                        remainingCount += length;
                        return;
                    }

                    // fill up buffer
                    Buffer.BlockCopy(bytes, offset, remainingBuffer, remainingCount, BufferSize - remainingCount);
                    start += BufferSize - remainingCount;
                    // now we have 64 bytes in buffer
                    MD5Core.GetHashBlock(remainingBuffer, ref abcdStruct, 0);
                    totalLength += BufferSize;
                    remainingCount = 0;
                }

                // while has 64 bytes blocks
                while (start <= length - BufferSize)
                {
                    MD5Core.GetHashBlock(bytes, ref abcdStruct, start);
                    totalLength += BufferSize;
                    start += BufferSize;
                }

                // save rest (if any)
                if (start != length)
                {
                    remainingCount = length - start;
                    Buffer.BlockCopy(bytes, start, remainingBuffer, 0, remainingCount);
                }

            }

            public int StorageHashSize
            {
                get
                {
                    return 20;
                }
            }

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

        public class FipsSymmetricalEncryptor : ISymmetricalEncryptor
        {
            private readonly SymmetricAlgorithm algorithm;

            public FipsSymmetricalEncryptor()
            {
                algorithm = Aes.Create();
            }

            public byte[] Key
            {
                get
                {
                    return algorithm.Key;
                }

                set
                {
                    algorithm.Key = value;
                }
            }

            public byte[] IV
            {
                get
                {
                    return algorithm.IV;
                }

                set
                {
                    algorithm.IV = value;
                }
            }

            public int KeySize
            {
                get
                {
                    return algorithm.KeySize;
                }

                set
                {
                    algorithm.KeySize = value;
                }
            }

            public void GenerateKey()
            {
                algorithm.GenerateKey();
            }

            public void GenerateIV()
            {
                algorithm.GenerateIV();
            }

            public ICryptoTransform CreateEncryptor()
            {
                return algorithm.CreateEncryptor();
            }

            public ICryptoTransform CreateDecryptor()
            {
                return algorithm.CreateDecryptor();
            }

            public ICryptoTransform CreateDecryptor(byte[] key, byte[] iv)
            {
                return algorithm.CreateDecryptor(key, iv);
            }

            public void Dispose()
            {
                if (algorithm != null)
                    algorithm.Dispose();
            }
        }
        public class FipsAsymmetricalEncryptor : IAsymmetricalEncryptor
        {
            private readonly RSACng algorithm;

            public FipsAsymmetricalEncryptor()
            {
                
                algorithm = new RSACng();
            }

            public FipsAsymmetricalEncryptor(int keySize)
            {
                algorithm = new RSACng(keySize);
            }

            public int KeySize
            {
                get
                {
                    return algorithm.KeySize;
                }

                set
                {
                    algorithm.KeySize = value;
                }
            }

            public AsymmetricAlgorithm Algorithm
            {
                get
                {
                    return algorithm;
                }
            }

            public void ImportParameters(byte[] exponent, byte[] modulus)
            {
                algorithm.ImportParameters(new RSAParameters
                {
                    Modulus = modulus,
                    Exponent = exponent
                });
            }

            public byte[] Encrypt(byte[] bytes)
            {
                return algorithm.Encrypt(bytes, RSAEncryptionPadding.OaepSHA256);
            }

            public byte[] Decrypt(byte[] bytes)
            {
                return algorithm.Decrypt(bytes, RSAEncryptionPadding.OaepSHA256);
            }


            public void ImportParameters(RSAParameters parameters)
            {
                algorithm.ImportParameters(parameters);
            }

            public RSAParameters ExportParameters(bool includePrivateParameters)
            {
                return algorithm.ExportParameters(includePrivateParameters);
            }

            public byte[] SignHash(byte [] data)
            {
                return algorithm.SignHash(data, HashAlgorithmName.SHA1, RSASignaturePadding.Pkcs1);
            }

            public bool VerifyHash(byte[] hash, byte[] signature)
            {
                return algorithm.VerifyHash(hash, signature, HashAlgorithmName.SHA1, RSASignaturePadding.Pkcs1);
            }

            public void Dispose()
            {
                if (algorithm != null)
                    algorithm.Dispose();
            }
        }
    }

}
