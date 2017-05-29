using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FastTests.Server.NotificationCenter;
using Orders;
using Raven.Client.Documents;
using SlowTests.Smuggler;
using FastTests.Client.Subscriptions;
using System.Threading.Tasks;

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

                Parallel.For(0, 10, async _ =>
                {
                    using (var a = new SlowTests.Issues.RavenDB937())
                    {
                        await a.LowLevelEmbeddedStreamAsync();
                    }
                });
            }
        }
    }
}
