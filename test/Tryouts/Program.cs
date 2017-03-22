using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using FastTests.Server.Replication;

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
                using (var a = new FastTests.Issues.RavenDB_6064())
                {
                    a.MapReduceOnSeveralCompressedStrings();
                }
            }
        }
    }
}