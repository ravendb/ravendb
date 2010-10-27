using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Bundles.Tests.Expiration;
using Raven.Bundles.Tests.Replication;

namespace Raven.Bundles.Tryouts
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("starting...");
            for (int i = 0; i < 10000; i++)
            {
                using (var x = new SimpleReplication())
                {
                    x.Can_replicate_between_two_instances();
                }
                Console.Write(i + "\r");

            }
        }
    }
}
