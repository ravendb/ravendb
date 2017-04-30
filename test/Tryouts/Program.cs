using System;
using System.Diagnostics;
using SlowTests.Issues;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var a = new FastTests.Server.Replication.ReplicationResolveToDatabase())
                {
                    a.ResolveToTombstone().Wait();
                }
            }
        }
    }
}