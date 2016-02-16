using System;
using System.Security.Cryptography;
using System.Threading;

namespace metrics.Support
{
    /// <summary>
    /// Provides statistically relevant random number generation
    /// </summary>
    public class Random
    {
		private static readonly ThreadLocal<RandomNumberGenerator> _random = new ThreadLocal<RandomNumberGenerator>(RandomNumberGenerator.Create);
        
        public static long NextLong()
        {
            var buffer = new byte[sizeof(long)];
            _random.Value.GetBytes(buffer);
            var value = BitConverter.ToInt64(buffer, 0);
            return value;
        }
    }
}
