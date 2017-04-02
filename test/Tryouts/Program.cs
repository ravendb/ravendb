using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Issues;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);

                using (var a = new RavenDB_6602())
                {
                    a.RequestExecutor_failover_to_database_topology_should_work().Wait();                    
                }
            }
        }
    }
}