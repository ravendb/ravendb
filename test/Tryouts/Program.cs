using System;
using FastTests.Server.Replication;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);

                using (var a = new DisableDatabasePropagationInRaftCluster())
                {
                    a.DisableDatabaseToggleOperation_should_propagate_through_raft_cluster().Wait();                    
                }
            }
        }
    }
}