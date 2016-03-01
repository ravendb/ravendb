
using System;
using System.Threading;
namespace Metrics.Utils
{
    public static class ThreadLocalRandom
    {
        private static readonly ThreadLocal<Random> random = new ThreadLocal<Random>(() => new Random());

        public static double NextDouble()
        {
            return random.Value.NextDouble();
        }

        public static long NextLong()
        {
            long heavy = random.Value.Next();
            long light = random.Value.Next();

            return heavy << 32 | light;
        }
    }
}
