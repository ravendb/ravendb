using Sparrow;

namespace Raven.Client.Http
{
    internal static class ServerHash
    {
        public static string GetServerHash(string url)
        {
            return Hashing.XXHash64.CalculateRaw(url.ToLower()).ToString("X");
        }

        internal static string GetServerHash(string url, string database)
        {
            return Hashing.XXHash64.CalculateRaw(url.ToLower(), Hashing.XXHash64.CalculateRaw(database.ToLower())).ToString("X");
        }
    }
}
