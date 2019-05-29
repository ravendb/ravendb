using System;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using StressTests.Cluster;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 123; i++)
            {
                Console.WriteLine(i);
                try
                {
                    using (var test = new ClusterStressTests())
                    {
                        test.ParallelClusterTransactions().Wait();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
        }
    }
}
