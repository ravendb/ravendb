using System;
using System.Threading.Tasks;
using RachisTests;
using SlowTests.Server.Replication;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            try
            {
                Parallel.For(0, 5000, new ParallelOptions
                {
                    MaxDegreeOfParallelism = 16
                }, _ =>
                {
                    Console.Write(".");

                    using (var test = new ReplicationBasicTestsSlow())
                    {
                        test.DisableExternalReplication().Wait();
                    }
                });
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}
