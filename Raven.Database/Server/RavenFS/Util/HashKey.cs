using System.Security.Cryptography;

namespace Raven.Database.Server.RavenFS.Util
{
	public class HashKey
	{
		public HashKey(byte[] buffer, int size)
		{
			using (var sha256 = SHA256.Create())
			{
				Strong = sha256.ComputeHash(buffer, 0, size);
				Weak = new RabinKarpHasher(size).Init(buffer, 0, size);
			}
		}

		public HashKey()
		{

		}

		public byte[] Strong { get; set; }
		public int Weak { get; set; }
	}
}