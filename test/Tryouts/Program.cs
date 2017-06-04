using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Client.Attachments;
using FastTests.Tasks;
using RachisTests.DatabaseCluster;
using Sparrow.Logging;
using System.Threading.Tasks;
using Raven.Server.Utils;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            MiscUtils.DisableLongTimespan = true;
            LoggingSource.Instance.SetupLogMode(LogMode.Information, @"c:\work\debug\ravendb");

            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                //Parallel.For(0, 10, _ =>
                //{
                    using (var a = new RavenDB_6886())
                    {
                        Console.Write(".");
                        a.Cluster_identity_for_single_document_in_parallel_on_different_nodes_should_work().Wait();
                    }
                //});
                Console.WriteLine();
            }
        }
    }
}
