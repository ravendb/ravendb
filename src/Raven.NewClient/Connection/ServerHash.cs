using Sparrow;

namespace Raven.NewClient.Client.Connection
{
    public static class ServerHash
    {
        public static string GetServerHash(string url)
        {
            return Hashing.XXHash64.CalculateRaw(url).ToString("X");
        }

        internal static string GetServerHash(string url, string database)
        {
            return Hashing.XXHash64.CalculateRaw(url, Hashing.XXHash64.CalculateRaw(database)).ToString("X");
        }
    }
}
