using System;
using System.Text;

using Raven.Abstractions.Util.Encryptors;
using Sparrow;

namespace Raven.Client.Connection
{
	public static class ServerHash
	{
		public static string GetServerHash(string url)
		{			
            var hash = Hashing.XXHash64.CalculateRaw(url);
            return hash.ToString("X");
		}
	}
}