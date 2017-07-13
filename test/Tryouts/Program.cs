using System;
using SlowTests.Bugs;
using SlowTests.Issues;
using FastTests.Voron.Storage;
using SlowTests.Cluster;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {   
                Console.WriteLine(i);
                using (var test = new ClusterModesForRequestExecutorTest())   
                {
                    test.Round_robin_load_balancing_with_failing_node_should_work().Wait();
                }
            }
        }
    }
}
