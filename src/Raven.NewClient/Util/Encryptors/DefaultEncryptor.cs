// -----------------------------------------------------------------------
//  <copyright file="DefaultEncryptor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Security.Cryptography;

using Sparrow;

namespace Raven.NewClient.Abstractions.Util.Encryptors
{
    public sealed class DefaultEncryptor : EncryptorBase<DefaultEncryptor.DefaultHashEncryptor, FipsEncryptor.FipsSymmetricalEncryptor, FipsEncryptor.FipsAsymmetricalEncryptor>
    {
        public DefaultEncryptor()
        {
            Hash = new DefaultHashEncryptor(allowNonThreadSafeMethods: false);
        }

        public override IHashEncryptor Hash { get; protected set; }

        public class DefaultHashEncryptor : HashEncryptorBase, IHashEncryptor
        {
            private readonly ObjectPool<MD5> md5Pool = new ObjectPool<MD5>(() => MD5.Create(), 16);
            private readonly ObjectPool<SHA1> sha1Pool = new ObjectPool<SHA1>(() => SHA1.Create(), 16);
            private readonly ObjectPool<SHA256> sha256Pool = new ObjectPool<SHA256>(() => SHA256.Create(), 16);

            public DefaultHashEncryptor()
                : this(true)
            {
            }

            public DefaultHashEncryptor(bool allowNonThreadSafeMethods)
                : base(allowNonThreadSafeMethods)
            {
            }

            public int StorageHashSize
            {
                get { return 32; }
            }

            public void Dispose()
            {
            }

            public byte[] ComputeForStorage(byte[] bytes)
            {
                SHA256 algorithm = null;
                try
                {
                    algorithm = this.sha256Pool.Allocate();
                    return ComputeHashInternal(algorithm, bytes);
                }
                finally
                {
                    if (algorithm != null)
                        this.sha256Pool.Free(algorithm);
                }                
            }

            public byte[] ComputeForStorage(byte[] bytes, int offset, int length)
            {
                SHA256 algorithm = null;
                try
                {
                    algorithm = this.sha256Pool.Allocate();
                    return ComputeHashInternal(algorithm, bytes, offset, length);
                }
                finally
                {
                    if (algorithm != null)
                        this.sha256Pool.Free(algorithm);
                }
            }

            public byte[] ComputeForOAuth(byte[] bytes)
            {
                SHA1 algorithm = null;
                try
                {
                    algorithm = this.sha1Pool.Allocate();
                    return ComputeHashInternal(algorithm, bytes);
                }
                finally
                {
                    if (algorithm != null)
                        this.sha1Pool.Free(algorithm);
                }
            }

            public byte[] Compute16(byte[] bytes)
            {
                if (bytes.Length < 512)
                    return MD5Core.GetHash(bytes);

                MD5 algorithm = null;
                try
                {
                    algorithm = this.md5Pool.Allocate();
                    return ComputeHashInternal(algorithm, bytes);
                }
                finally
                {
                    if (algorithm != null)
                        this.md5Pool.Free(algorithm);
                }
            }

            public byte[] Compute16(Stream stream)
            {
                MD5 algorithm = null;
                try
                {
                    algorithm = this.md5Pool.Allocate();
                    return algorithm.ComputeHash(stream);
                }
                finally
                {
                    if (algorithm != null)
                        this.md5Pool.Free(algorithm);
                }
            }

            public byte[] Compute16(byte[] bytes, int offset, int length)
            {
                if (bytes.Length < 512)
                    return MD5Core.GetHash(bytes, offset, length);

                MD5 algorithm = null;
                try
                {
                    algorithm = this.md5Pool.Allocate();
                    return ComputeHashInternal(algorithm, bytes, offset, length);
                }
                finally
                {
                    if (algorithm != null)
                        this.md5Pool.Free(algorithm);
                }
            }

            public byte[] Compute20(byte[] bytes)
            {
                SHA1 algorithm = null;
                try
                {
                    algorithm = this.sha1Pool.Allocate();
                    return ComputeHashInternal(algorithm, bytes);
                }
                finally
                {
                    if (algorithm != null)
                        this.sha1Pool.Free(algorithm);
                }
            }

            public byte[] Compute20(byte[] bytes, int offset, int length)
            {
                SHA1 algorithm = null;
                try
                {
                    algorithm = this.sha1Pool.Allocate();
                    return ComputeHashInternal(algorithm, bytes, offset, length);
                }
                finally
                {
                    if (algorithm != null)
                        this.sha1Pool.Free(algorithm);
                }
            }
        }
    }
}
