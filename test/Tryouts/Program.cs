using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Orders;
using Raven.Client.Documents;
using SlowTests.Smuggler;
using RachisTests;
using Sparrow.Logging;

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
                using (var a = new SubscriptionsFailover())
                {
                    a.ContinueFromThePointIStopped().Wait();
                }
            }
        }
    }
}
