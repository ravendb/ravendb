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
		private readonly int preferedEncryptionKeyBitsSize;		

		public readonly Codec Codec;

		public EncryptionSettings(byte[] encryptionKey, Type symmetricAlgorithmType, bool encryptIndexes,int preferedEncryptionKeyBitsSize)
		{
			EncryptionKey = encryptionKey;
			this.encryptIndexes = encryptIndexes;

			Codec = new Codec(this);

			algorithmType = symmetricAlgorithmType;
			this.preferedEncryptionKeyBitsSize = preferedEncryptionKeyBitsSize;
			algorithmGenerator = Expression.Lambda<Func<SymmetricAlgorithm>>
				(Expression.New(symmetricAlgorithmType)).Compile();
		}

		public static bool DontEncrypt(string key)
		{
			return key.StartsWith(Constants.DontEncryptDocumentsStartingWith, StringComparison.OrdinalIgnoreCase)
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

		public int PreferedEncryptionKeyBitsSize
		{
			get { return preferedEncryptionKeyBitsSize; }
		}

		public static byte[] GenerateRandomEncryptionKey(int length)
		{
			var result = new byte[length];
			RandomNumberGenerator.Create().GetBytes(result);
			return result;
		}
	}
}
