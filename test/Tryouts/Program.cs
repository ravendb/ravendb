using System;
using System.Threading.Tasks;
using FastTests.Voron.Bugs;
using SlowTests.Server.Documents.PeriodicBackup;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {

            for (int i = 0; i < 10000; i++)
            {
                Console.WriteLine(i);
                Parallel.For(0, 10, _ =>
                {
                    using (var test = new FastTests.Server.Documents.Indexing.Static.BasicStaticMapReduceIndexing())
                    {
                        try
                        {
                            test.CanPersist().Wait();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            Console.WriteLine("-------------");
                            throw;
                        }
                    }
                });
            }
        }
    }
}
