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
			Hash = new DefaultHashEncryptor();
		}

		public override IHashEncryptor Hash { get; protected set; }

		public class DefaultHashEncryptor : HashEncryptorBase, IHashEncryptor
		{
			public int StorageHashSize
			{
				get
				{
					return 32;
				}
			}

		    private MD5 md5;

		    public void TransformBlock(byte[] bytes, int offset, int length)
		    {
                if (md5 == null)
                    md5 = MD5.Create();

		        md5.TransformBlock(bytes, offset, length, null, 0);
		    }

		    public byte[] TransformFinalBlock()
		    {
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
				return ComputeHash(SHA256.Create(), bytes);
			}

            public byte[] ComputeForStorage(byte[] bytes, int offset, int length)
            {
                return ComputeHash(SHA256.Create(), bytes, offset, length);
            }

			public byte[] ComputeForOAuth(byte[] bytes)
			{
				return ComputeHash(SHA1.Create(), bytes);
			}

			public byte[] Compute16(byte[] bytes)
			{
				return ComputeHash(MD5.Create(), bytes);
			}

		    public byte[] Compute16(Stream stream)
		    {
		        using (var hasher = MD5.Create())
		        {
                    return hasher.ComputeHash(stream);
		        }
		    }

		    public byte[] Compute16(byte[] bytes, int offset, int length)
            {
                return ComputeHash(MD5.Create(), bytes, offset, length);
            }

			public byte[] Compute20(byte[] bytes)
			{
				return ComputeHash(SHA1.Create(), bytes);
			}

            public byte[] Compute20(byte[] bytes, int offset, int length)
            {
                return ComputeHash(SHA1.Create(), bytes, offset, length);
            }
		}
	}
}