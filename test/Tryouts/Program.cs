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
                Parallel.For(0, 1, _ =>
                {
                    using (var a = new FastTests.Server.NotificationCenter.NotificationCenterTests())
                    {
                        a.Should_get_notification();
                    }
                });
                //LoggingSource.Instance.SetupLogMode(LogMode.None, "logs");
                //Directory.Delete("logs", true);

            }
        }
    }
}