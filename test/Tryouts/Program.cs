using System;
using SlowTests.Bugs;
using SlowTests.Issues;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {   
                Console.WriteLine(i);
                using (var test = new RachisTests.DatabaseCluster.ClusterDatabaseMaintenance())   
                {
                    test.MoveToPassiveWhenRefusedConnectionFromAllNodes().Wait();
                }
            }
        }
    }
}
