using System;
using System.Diagnostics;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            using (var a = new FastTests.Server.Documents.Indexing.MapReduce.RavenDB_4323_Replication())
            {
                a.ReduceOutputShouldNotBeReplicated().Wait();
            }
        }
    }
}