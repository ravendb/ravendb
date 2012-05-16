using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Bundles.Encryption
{
	[Serializable]
	public class EncryptionSettings : ISerializable
	{
		private byte[] encryptionKey;
		private Type algorithmType;
		private Func<SymmetricAlgorithm> algorithmGenerator;

		public EncryptionSettings()
			: this(GenerateRandomEncryptionKey())
		{
		}

		public EncryptionSettings(byte[] encryptionKey)
			: this(encryptionKey, Constants.DefaultCryptoServiceProvider)
		{
		}

		public EncryptionSettings(byte[] encryptionKey, Type symmetricAlgorithmType)
		{
			this.EncryptionKey = encryptionKey;

			typeof(EncryptionSettings)
				.GetMethod("SetSymmetricAlgorithmType", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
				.MakeGenericMethod(symmetricAlgorithmType)
				.Invoke(this, new object[0]);
		}

		public static bool DontEncrypt(string key)
		{
			return key.StartsWith(Constants.DontEncryptDocumentsStartingWith, StringComparison.InvariantCultureIgnoreCase);
		}

		public byte[] EncryptionKey
		{
			get { return encryptionKey; }
			set
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

		protected EncryptionSettings(SerializationInfo info, StreamingContext context)
			: this((byte[])info.GetValue("encryptionKey", typeof(byte[])),
				(Type)info.GetValue("algorithmType", typeof(Type))) { }

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context)
		{
			info.AddValue("encryptionKey", encryptionKey);
			info.AddValue("algorithmType", algorithmType);
		}
	}
}
