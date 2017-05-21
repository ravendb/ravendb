using System;
using System.Diagnostics;
using FastTests.Client.Indexing;
using FastTests.Client.Subscriptions;
using FastTests.Server.Documents;
using FastTests.Server.Documents.Queries;
using SlowTests.Issues;
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

            for (int i = 0; i < 800; i++)
            {
                Console.WriteLine(i);

                using (var a = new FastTests.Client.IndexesDeleteByIndexTests())
                {
                    a.Delete_By_Index_Async().Wait();
                }
            }

            
        }
    }
}