using Sparrow;

namespace Raven.Client.Http
{
    internal static class TopologyHash
    {
        public static string GetTopologyHash(string[] initialUrls)
        {
            var key = string.Join(",", initialUrls).ToLower();
            return Hashing.XXHash64.CalculateRaw(key).ToString("X");
        }
    }
}
