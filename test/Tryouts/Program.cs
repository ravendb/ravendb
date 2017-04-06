using System;
using System.Threading.Tasks;
using FastTests.Issues;
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

                Parallel.For(0, 1, _ =>
                {
                    using (var a = new FastTests.Issues.RavenDB_6602())
                    {
                        a.RequestExecutor_failover_with_only_one_database_should_properly_fail().Wait();
                    }
                });
            }
        }
    }
}