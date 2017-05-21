using System;
using System.Diagnostics;
using FastTests.Client.Indexing;
using FastTests.Client.Subscriptions;
using FastTests.Server.Documents;
using FastTests.Server.Documents.Queries;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Smuggler;
using SlowTests.Tests.Faceted;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            for (int i = 0; i < 800; i++)
            {
                Console.WriteLine(i);

                using (var a = new LegacySmugglerTests())
                {
                    a.CanImportIndexesAndTransformers("SlowTests.Smuggler.Indexes_And_Transformers_3.5.ravendbdump").Wait();
                }
            }
        }
    }
}