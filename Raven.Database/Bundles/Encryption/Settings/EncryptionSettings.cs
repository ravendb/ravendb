using System;
using System.Security.Cryptography;

namespace Raven.Bundles.Encryption.Settings
{
	public class EncryptionSettings
	{
		private byte[] encryptionKey;
		private Type algorithmType;
		private Func<SymmetricAlgorithm> algorithmGenerator;
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
			this.EncryptionKey = encryptionKey;
			this.encryptIndexes = encryptIndexes;

			this.Codec = new Codec(this);

			typeof(EncryptionSettings)
				.GetMethod("SetSymmetricAlgorithmType", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
				.MakeGenericMethod(symmetricAlgorithmType)
				.Invoke(this, new object[0]);
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
					throw new ArgumentNullException("EncryptionKey");

				if (value.Length < Constants.MinimumAcceptableEncryptionKeyLength)
					throw new ArgumentException("The EncryptionKey provided is too short. The minimum length is " + Constants.MinimumAcceptableEncryptionKeyLength + ".");
				encryptionKey = value;
			}
		}

		private void SetSymmetricAlgorithmType<T>() where T : SymmetricAlgorithm, new()
		{
			algorithmGenerator = () => new T();
			algorithmType = typeof(T);
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
			byte[] result = new byte[length];
			RNGCryptoServiceProvider.Create().GetBytes(result);
			return result;
		}
	}
}
