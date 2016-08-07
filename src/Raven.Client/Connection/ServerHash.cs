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

        internal static string GetServerHash(string url, string database)
        {
            // TODO: avoid string allocation
            return GetServerHash(url + database);
        }
    }
}
