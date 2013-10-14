namespace Raven.Abstractions.Util.Encryptors
{
	using System;

	public abstract class EncryptorBase<THashEncryptor, TSymmetricalEncryptor, TAsymmetricalEncryptor> : IEncryptor
		where THashEncryptor : IHashEncryptor, new()
		where TSymmetricalEncryptor : ISymmetricalEncryptor, new()
		where TAsymmetricalEncryptor : IAsymmetricalEncryptor, new()
	{
		public abstract IHashEncryptor Hash { get; protected set; }

		public IHashEncryptor CreateHash()
		{
			return new THashEncryptor();
		}

		public ISymmetricalEncryptor CreateSymmetrical()
		{
			return new TSymmetricalEncryptor();
		}

		public ISymmetricalEncryptor CreateSymmetrical(int keySize)
		{
			var algorithm = CreateSymmetrical();
			algorithm.KeySize = keySize;

			return algorithm;
		}

		public IAsymmetricalEncryptor CreateAsymmetrical()
		{
			return new TAsymmetricalEncryptor();
		}

		public IAsymmetricalEncryptor CreateAsymmetrical(byte[] exponent, byte[] modulus)
		{
			var algorithm = CreateAsymmetrical();
			algorithm.ImportParameters(exponent, modulus);

			return algorithm;
		}

		public IAsymmetricalEncryptor CreateAsymmetrical(int keySize)
		{
			return (IAsymmetricalEncryptor)Activator.CreateInstance(typeof(TAsymmetricalEncryptor), keySize);
		}
	}
}