namespace Raven.Abstractions.Util.Encryptors
{
	using System;
	using System.Security.Cryptography;

	public abstract class HashEncryptorBase
	{
		public byte[] ComputeHash(HashAlgorithm algorithm, byte[] bytes, int? size = null)
		{
			using (algorithm)
			{
				var hash = algorithm.ComputeHash(bytes);
				if (size.HasValue)
					Array.Resize(ref hash, size.Value);

				return hash;
			}
		}
	}
}