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
                    using (var a = new SlowTests.Issues.RavenDB_5500())
                    {
                        a.WillNotLoadTwoIndexesWithTheSameId().Wait();
                    }
                });
            }
        }
    }
}