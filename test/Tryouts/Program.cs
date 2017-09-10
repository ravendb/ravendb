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
                using (var test = new SlowTests.Server.Replication.ReplicationBasicTestsSlow())
                {
                    try
                    {
                        test.Master_master_replication_from_etag_zero_without_conflict_should_work(true).Wait();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Console.WriteLine("-------------");
                        throw;
                    }
                }
            }
        }
    }
}
