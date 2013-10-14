// -----------------------------------------------------------------------
//  <copyright file="DefaultEncryptor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Security.Cryptography;
#if !SILVERLIGHT
	using System.Security.Cryptography;
#else
using Raven.Abstractions.Util;
#endif

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
#if !SILVERLIGHT
					return 32;
#else
					throw new NotSupportedException();
#endif
				}
			}

			public byte[] ComputeForStorage(byte[] bytes)
			{
#if !SILVERLIGHT
				return ComputeHash(SHA256.Create(), bytes);
#else
				throw new NotSupportedException();
#endif
			}

			public byte[] ComputeForOAuth(byte[] bytes)
			{
#if !SILVERLIGHT
				return ComputeHash(SHA1.Create(), bytes);
#else
				throw new NotSupportedException();
#endif
			}

			public byte[] Compute16(byte[] bytes)
			{
#if !SILVERLIGHT
				return ComputeHash(MD5.Create(), bytes);
#else
				return MD5Core.GetHash(bytes);
#endif
			}

			public byte[] Compute20(byte[] bytes)
			{
#if !SILVERLIGHT
				return ComputeHash(SHA1.Create(), bytes);
#else
				return MD5Core.GetHash(bytes);
#endif
			}
		}
	}
}