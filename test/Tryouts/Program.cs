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
                LoggingSource.Instance.SetupLogMode(LogMode.Information, "logs");
                Parallel.For(0, 10, _ =>
                {
                    using (var a = new BasicTests())
                    {
                        a.CanApplyCommitAcrossAllCluster(amount: 7).Wait();
                    }
                });
                LoggingSource.Instance.SetupLogMode(LogMode.None, "logs");
                Directory.Delete("logs", true);

            }
        }
    }
}