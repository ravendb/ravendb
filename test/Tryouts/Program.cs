using System;
using System.Diagnostics;
using FastTests.Client.Indexing;
using FastTests.Client.Subscriptions;
using FastTests.Server.Documents.Queries;
using SlowTests.MailingList;
using SlowTests.Tests.Faceted;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            for (int i = 0; i < 199; i++)
            {
                Console.WriteLine(i);

                using (var a = new WaitingForNonStaleResults())
                {
                    a.Throws_if_exceeds_timeout();
                }

                using (var a = new FastTests.Server.Replication.ReplicationIndexesAndTransformers())
                {
                    a.Can_replicate_multiple_indexes().Wait();
                }
            }
        }
    }
}