using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using RachisTests.DatabaseCluster;
using Sparrow.Logging;

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

                using (var a = new AttachmentFailover())
                {
                    a.PutAttachmentsWithFailover(1024 * 1024 * 1).Wait();
                }
            }
        }
    }
}
