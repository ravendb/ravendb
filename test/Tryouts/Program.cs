using System;
using FastTests.Server;
using FastTests.Server.Documents.Indexing;
using FastTests.Server.Documents.Queries.Dynamic.Map;
using SlowTests.Bugs;
using SlowTests.Issues;
using FastTests.Voron.Storage;
using SlowTests.Cluster;
using Raven.Server.Documents.Replication;
using Raven.Client.Documents;
using SlowTests.Client.Subscriptions;
using SlowTests.Tests.Linq;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //using (var store = new DocumentStore
            //{
            //    Urls = new string[] { "http://192.168.0.100:8080" },
            //    Database = "Demo"
            //}.Initialize())
            //{
            //    store.SetRequestsTimeout(TimeSpan.FromSeconds(1));

            //    while (true)
            //    {
            //        using (var session = store.OpenSession())
            //        {
            //            session.Load<dynamic>("users/1");
            //            Console.WriteLine("a");
            //        }
            //        Console.ReadLine();
            //    }

            //    //var sub = store.Subscriptions.Open(new Raven.Client.Documents.Subscriptions.SubscriptionConnectionOptions(11));
            //    //sub.Run(batch =>
            //    //{
            //    //    foreach (var item in batch.Items)
            //    //    {
            //    //        Console.WriteLine(item.Id);
            //    //    }
            //    //}).Wait();
            //}
            RunTest();
        }

        private static void RunTest()
        {
            for (int i = 0; i < 100; i++)
            {
                Console.Clear();
                Console.WriteLine(i);
                using (var test = new FastTests.Blittable.PartialParsingBugs())
                {
                    try
                    {
                        test.TestOneCharAtATime("{\"Neg1\":-9223372036854775808,\"Neg\":-6}");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Console.Beep();
                        return;
                    }
                }
            }
        }
    }
}
