// -----------------------------------------------------------------------
//  <copyright file="FipsEncryptor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Security.Cryptography;

namespace Raven.Abstractions.Util.Encryptors
{
	public sealed class FipsEncryptor : EncryptorBase<FipsEncryptor.FipsHashEncryptor, FipsEncryptor.FipsSymmetricalEncryptor, FipsEncryptor.FipsAsymmetricalEncryptor>
	{
		public FipsEncryptor()
		{
			Hash = new FipsHashEncryptor();
		}

		public override IHashEncryptor Hash { get; protected set; }

		public class FipsHashEncryptor : HashEncryptorBase, IHashEncryptor
		{
			public int StorageHashSize
			{
				get
				{
#if !SILVERLIGHT
					return 20;
#else
					throw new NotSupportedException();
#endif
				}
			}

			//SHA1
			public byte[] ComputeForStorage(byte[] bytes)
			{
				if(StorageHashSize == 20)
					return Compute20(bytes);
				return Compute16(bytes);
			}

			public byte[] ComputeForOAuth(byte[] bytes)
			{
				return Compute16(bytes);
			}

			public byte[] Compute16(byte[] bytes)
			{
				return MD5Core.GetHash(bytes);
			}

			public byte[] Compute20(byte[] bytes)
			{
#if !SILVERLIGHT
				return ComputeHash(SHA1.Create(), bytes, 20);
#else
				return ComputeHash(new SHA1Managed(), bytes, 20);
#endif		
			}
		}

		public class FipsSymmetricalEncryptor : ISymmetricalEncryptor
		{
			private readonly SymmetricAlgorithm algorithm;

			public FipsSymmetricalEncryptor()
			{
#if !SILVERLIGHT
				algorithm = new AesCryptoServiceProvider();
#else
				algorithm = new AesManaged();
#endif
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
#if !SILVERLIGHT
				if (algorithm != null)
					algorithm.Dispose();
#endif
			}
		}
#if !SILVERLIGHT
		public class FipsAsymmetricalEncryptor : IAsymmetricalEncryptor
		{
			private readonly RSACryptoServiceProvider algorithm;

			public FipsAsymmetricalEncryptor()
			{
				algorithm = new RSACryptoServiceProvider();
			}

			public FipsAsymmetricalEncryptor(int keySize)
			{
				algorithm = new RSACryptoServiceProvider(keySize);
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

			public byte[] Encrypt(byte[] bytes, bool fOAEP)
			{
				return algorithm.Encrypt(bytes, fOAEP);
			}

			public byte[] Decrypt(byte[] bytes, bool fOAEP)
			{
				return algorithm.Decrypt(bytes, fOAEP);
			}

			public void FromXmlString(string xml)
			{
				algorithm.FromXmlString(xml);
			}

			public void ImportCspBlob(byte[] keyBlob)
			{
				algorithm.ImportCspBlob(keyBlob);
			}

			public byte[] ExportCspBlob(bool includePrivateParameters)
			{
				return algorithm.ExportCspBlob(includePrivateParameters);
			}

			public byte[] SignHash(byte[] hash, string str)
			{
				return algorithm.SignHash(hash, str);
			}

			public bool VerifyHash(byte[] hash, string str, byte[] signature)
			{
				return algorithm.VerifyHash(hash, str, signature);
			}

			public void ImportParameters(RSAParameters parameters)
			{
				algorithm.ImportParameters(parameters);
			}

			public RSAParameters ExportParameters(bool includePrivateParameters)
			{
				return algorithm.ExportParameters(includePrivateParameters);
			}

			public void Dispose()
			{
				if (algorithm != null)
					algorithm.Dispose();
			}
		}
#else
		public class FipsAsymmetricalEncryptor : IAsymmetricalEncryptor
		{
			public int KeySize { get; set; }

			public void ImportParameters(byte[] exponent, byte[] modulus)
			{
				throw new System.NotImplementedException();
			}

			public byte[] Encrypt(byte[] bytes, bool fOAEP)
			{
				throw new System.NotImplementedException();
			}

			public void ImportCspBlob(byte[] keyBlob)
			{
				throw new System.NotImplementedException();
			}

			public byte[] ExportCspBlob(bool includePrivateParameters)
			{
				throw new System.NotImplementedException();
			}

			public byte[] SignHash(byte[] hash, string str)
			{
				throw new System.NotImplementedException();
			}

			public bool VerifyHash(byte[] hash, string str, byte[] signature)
			{
				throw new System.NotImplementedException();
			}

			public byte[] Decrypt(byte[] bytes, bool fOAEP)
			{
				throw new System.NotImplementedException();
			}

			public void FromXmlString(string xml)
			{
				throw new System.NotImplementedException();
			}

			public void Dispose()
			{
				throw new System.NotImplementedException();
			}
		}
#endif
	}

}