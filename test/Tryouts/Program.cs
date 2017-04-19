using System;
using System.Threading.Tasks;
using FastTests.Issues;
using FastTests.Server.Replication;
using Lucene.Net.Store;
using SlowTests.Server.Rachis;
using Sparrow.Logging;
using Directory = System.IO.Directory;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                //LoggingSource.Instance.SetupLogMode(LogMode.Information, "logs");
                Parallel.For(0, 10, _ =>
                {
                    using (var a = new RavenDB_6602())
                    {
                        a.RequestExecutor_failover_with_only_one_database_should_properly_fail().Wait();
                    }
                });
                //LoggingSource.Instance.SetupLogMode(LogMode.None, "logs");
                //Directory.Delete("logs", true);

            }
        }
    }
}