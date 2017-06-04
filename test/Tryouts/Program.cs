using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using RachisTests.DatabaseCluster;
using Sparrow.Logging;
using System.Threading.Tasks;
using FastTests.Server.Documents.Notifications;
using Raven.Server.Utils;
using Raven.Client.Documents;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string Name;
        }

        public static void Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "test"
            }.Initialize())
            {
                var sub = store.Subscriptions.Open(new Raven.Client.Documents.Subscriptions.SubscriptionConnectionOptions("subscriptions/test/7")
                {

                });

                sub.Subscribe(Console.WriteLine);
                
                sub.Start();

                Console.ReadLine();


            }
            //MiscUtils.DisableLongTimespan = true;
            //LoggingSource.Instance.SetupLogMode(LogMode.Information, @"c:\work\debug\ravendb");

            //Console.WriteLine(Process.GetCurrentProcess().Id);
            //Console.WriteLine();

            //for (int i = 0; i < 100; i++)
            //{
            //    Console.WriteLine(i);
            //    using (var a = new AttachmentFailover())
            //    {
            //        a.PutAttachmentsWithFailover(false, 512 * 1024, "BfKA8g/BJuHOTHYJ+A6sOt9jmFSVEDzCM3EcLLKCRMU=").Wait();
            //    }
            //}
        }
    }
}
