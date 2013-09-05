using System;
using System.Text;
#if SILVERLIGHT || NETFX_CORE
using Raven.Abstractions.Util;
#else
using System.Security.Cryptography;
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
#if SILVERLIGHT || NETFX_CORE
			return MD5Core.GetHash(bytes);
#else
			using (var md5 = MD5.Create())
			{
				return md5.ComputeHash(bytes);
			}
#endif
		}
	}
}