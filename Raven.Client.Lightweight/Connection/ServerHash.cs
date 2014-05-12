using System;
using System.Text;
#if NETFX_CORE
using Raven.Abstractions.Util;
#else
using System.Security.Cryptography;
using Raven.Abstractions.Util.Encryptors;

#endif

namespace Raven.Client.Connection
{
	public static class ServerHash
	{
		public static string GetServerHash(string url)
		{
			var bytes = Encoding.UTF8.GetBytes(url);
			return BitConverter.ToString(GetHash(bytes));
		}

		private static byte[] GetHash(byte[] bytes)
		{
#if NETFX_CORE
			return MD5Core.GetHash(bytes);
#else
		    return Encryptor.Current.Hash.Compute16(bytes);
#endif
		}
	}
}