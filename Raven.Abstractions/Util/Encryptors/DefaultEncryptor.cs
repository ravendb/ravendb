// -----------------------------------------------------------------------
//  <copyright file="DefaultEncryptor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Security.Cryptography;

namespace Raven.Abstractions.Util.Encryptors
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
            private ObjectPool<MD5> md5Pool = new ObjectPool<MD5>(() => MD5.Create());
            private ObjectPool<SHA1> sha1Pool = new ObjectPool<SHA1>(() => SHA1.Create());
            private ObjectPool<SHA256> sha256Pool = new ObjectPool<SHA256>(() => SHA256.Create());

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

			private MD5 md5;

			public void TransformBlock(byte[] bytes, int offset, int length)
			{
				ThrowNotSupportedExceptionForNonThreadSafeMethod();

				if (md5 == null)
					md5 = MD5.Create();

				md5.TransformBlock(bytes, offset, length, null, 0);
			}

			public byte[] TransformFinalBlock()
			{
				ThrowNotSupportedExceptionForNonThreadSafeMethod();

				if (md5 == null)
					md5 = MD5.Create();

				md5.TransformFinalBlock(new byte[0], 0, 0);
				return md5.Hash;
			}

			public void Dispose()
			{
				if (md5 != null)
					md5.Dispose();
			}

			public byte[] ComputeForStorage(byte[] bytes)
			{
                SHA256 algorithm = null;
                try
                {
                    algorithm = this.sha256Pool.Get();
                    return ComputeHashInternal(algorithm, bytes);
                }
                finally
                {
                    if (algorithm != null)
                        this.sha256Pool.Put(algorithm);
                }                
			}

			public byte[] ComputeForStorage(byte[] bytes, int offset, int length)
			{
                SHA256 algorithm = null;
                try
                {
                    algorithm = this.sha256Pool.Get();
                    return ComputeHashInternal(algorithm, bytes, offset, length);
                }
                finally
                {
                    if (algorithm != null)
                        this.sha256Pool.Put(algorithm);
                }
			}

			public byte[] ComputeForOAuth(byte[] bytes)
			{
                SHA1 algorithm = null;
                try
                {
                    algorithm = this.sha1Pool.Get();
                    return ComputeHashInternal(algorithm, bytes);
                }
                finally
                {
                    if (algorithm != null)
                        this.sha1Pool.Put(algorithm);
                }
			}

			public byte[] Compute16(byte[] bytes)
			{
                if (bytes.Length < 512)
                    return MD5Core.GetHash(bytes);

                MD5 algorithm = null;
                try
                {
                    algorithm = this.md5Pool.Get();
                    return ComputeHashInternal(algorithm, bytes);
                }
                finally
                {
                    if (algorithm != null)
                        this.md5Pool.Put(algorithm);
                }
			}

            public byte[] Compute16(Stream stream)
			{
                MD5 algorithm = null;
                try
                {
                    algorithm = this.md5Pool.Get();
                    return algorithm.ComputeHash(stream);
                }
                finally
                {
                    if (algorithm != null)
                        this.md5Pool.Put(algorithm);
                }
			}

			public byte[] Compute16(byte[] bytes, int offset, int length)
			{
                if (bytes.Length < 512)
                    return MD5Core.GetHash(bytes, offset, length);

                MD5 algorithm = null;
                try
                {
                    algorithm = this.md5Pool.Get();
                    return ComputeHashInternal(algorithm, bytes, offset, length);
                }
                finally
                {
                    if (algorithm != null)
                        this.md5Pool.Put(algorithm);
                }
			}

			public byte[] Compute20(byte[] bytes)
			{
                SHA1 algorithm = null;
                try
                {
                    algorithm = this.sha1Pool.Get();
                    return ComputeHashInternal(algorithm, bytes);
                }
                finally
                {
                    if (algorithm != null)
                        this.sha1Pool.Put(algorithm);
                }
			}

			public byte[] Compute20(byte[] bytes, int offset, int length)
			{
                SHA1 algorithm = null;
                try
                {
                    algorithm = this.sha1Pool.Get();
                    return ComputeHashInternal(algorithm, bytes, offset, length);
                }
                finally
                {
                    if (algorithm != null)
                        this.sha1Pool.Put(algorithm);
                }
			}
		}
	}
}