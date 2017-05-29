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
                using (var a = new RavenDB937())
                {
                    a.LowLevelEmbeddedStreamAsync().Wait();
                }
            }
        }
    }
}
