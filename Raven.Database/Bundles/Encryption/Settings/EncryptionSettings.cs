using System;
using System.Security.Cryptography;
using Raven.Abstractions.Data;
using Expression = System.Linq.Expressions.Expression;

namespace Raven.Bundles.Encryption.Settings
{
	public class EncryptionSettings
	{
		private byte[] encryptionKey;
		private readonly Type algorithmType;
		private readonly Func<SymmetricAlgorithm> algorithmGenerator;
		private readonly bool encryptIndexes;

		public readonly Codec Codec;

		public EncryptionSettings()
			: this(GenerateRandomEncryptionKey())
		{
		}

		public EncryptionSettings(byte[] encryptionKey)
			: this(encryptionKey, Constants.DefaultCryptoServiceProvider)
		{
		}

		public EncryptionSettings(byte[] encryptionKey, Type symmetricAlgorithmType)
			: this(encryptionKey, symmetricAlgorithmType, true)
		{
		}

		public EncryptionSettings(byte[] encryptionKey, Type symmetricAlgorithmType, bool encryptIndexes)
		{
			EncryptionKey = encryptionKey;
			this.encryptIndexes = encryptIndexes;

			Codec = new Codec(this);

			algorithmType = symmetricAlgorithmType;

			algorithmGenerator = Expression.Lambda<Func<SymmetricAlgorithm>>(
				Expression.New(symmetricAlgorithmType)
				).Compile();
		}

		public static bool DontEncrypt(string key)
		{
			return key.StartsWith(Constants.DontEncryptDocumentsStartingWith, StringComparison.InvariantCultureIgnoreCase)
				&& key != Constants.InDatabaseKeyVerificationDocumentName;
		}

		public byte[] EncryptionKey
		{
			get { return encryptionKey; }
			private set
			{
				if (value == null)
					throw new ArgumentNullException("value");

				if (value.Length < Constants.MinimumAcceptableEncryptionKeyLength)
					throw new ArgumentException("The EncryptionKey provided is too short. The minimum length is " + Constants.MinimumAcceptableEncryptionKeyLength + ".");
				encryptionKey = value;
			}
		}


		public Type SymmetricAlgorithmType
		{
			get { return algorithmType; }
		}

		public Func<SymmetricAlgorithm> GenerateAlgorithm
		{
			get { return algorithmGenerator; }
		}

		public bool EncryptIndexes
		{
			get { return encryptIndexes; }
		}

		public static byte[] GenerateRandomEncryptionKey()
		{
			return GenerateRandomEncryptionKey(Constants.DefaultGeneratedEncryptionKeyLength);
		}

		public static byte[] GenerateRandomEncryptionKey(int length)
		{
			var result = new byte[length];
			RandomNumberGenerator.Create().GetBytes(result);
			return result;
		}
	}
}
