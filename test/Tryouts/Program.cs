using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FastTests.Server.NotificationCenter;
using FastTests.Server.Replication;
using Orders;
using Raven.Client.Documents;
using SlowTests.Issues;
using SlowTests.Smuggler;
using System.Threading.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            for (int i = 0; i < 1000000; i++)
            {
                Console.WriteLine(i);
                Parallel.For(0, 10, _ =>
                {
                    using (var a = new FastTests.Issues.RavenDB_5610())
                    {
                        a.WillUpdate().Wait();
                    }
                });
            }
        }
    }
}
