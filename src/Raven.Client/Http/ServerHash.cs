using Sparrow;

namespace Raven.Client.Http
{
    internal static class ServerHash
    {
        public static string GetServerHash(string url)
        {
            return Hashing.XXHash64.CalculateRaw(url.ToLower()).ToString("X");
        }
    }
}
