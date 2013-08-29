// -----------------------------------------------------------------------
//  <copyright file="DefaultEncryptor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Util.Encryptors
{
	using System.Security.Cryptography;

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

			public byte[] ComputeForStorage(byte[] bytes)
			{
				return ComputeHash(SHA256.Create(), bytes);
			}

			public byte[] ComputeForOAuth(byte[] bytes)
			{
				return ComputeHash(SHA1.Create(), bytes);
			}

			public byte[] Compute(byte[] bytes)
			{
				return ComputeHash(MD5.Create(), bytes);
			}
		}
	}
}