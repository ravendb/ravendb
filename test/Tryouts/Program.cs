using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using RachisTests.DatabaseCluster;
using Sparrow.Logging;
using System.Threading.Tasks;
using RachisTests;
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

            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var a = new AddNodeToClusterTests())
                {
                    a.RemoveNodeWithDb().Wait();
                }
            }
        }
    }
}
