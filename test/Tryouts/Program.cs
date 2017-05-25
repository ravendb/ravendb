using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Orders;
using Raven.Client.Documents;
using SlowTests.Smuggler;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var a = new FastTests.Server.Replication.DisableDatabasePropagationInRaftCluster())
                {
                    try
                    {
                        a.DisableDatabaseToggleOperation_should_propagate_through_raft_cluster().Wait();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Console.ReadLine();
                    }
                }
            }
        }
    }
}
