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
                Parallel.For(0, 1, _ =>
                {
                    using (var test = new SlowTests.Client.BulkInserts())
                    {
                        try
                        {
                            test.Simple_Bulk_Insert_With_Ssl().Wait();
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
