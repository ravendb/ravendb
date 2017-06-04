using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using RachisTests.DatabaseCluster;
using Sparrow.Logging;
using System.Threading.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            LoggingSource.Instance.SetupLogMode(LogMode.Information, @"c:\work\debug\ravendb");

            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);


                //Parallel.For(0, 10, _ =>
                {
                    using (var a = new FastTests.Client.Subscriptions.CriteriaScript())
                    {
                        a.CriteriaScriptWithTransformation(useSsl: false).Wait();
                    }
                }
                
                //);
            }
        }
    }
}
