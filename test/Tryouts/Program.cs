using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Queries.Parser;
using FastTests.Voron.Compaction;
using SlowTests.Authentication;
using SlowTests.Bugs.MapRedue;
using SlowTests.Client;
using SlowTests.Issues;
using SlowTests.MailingList;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    Console.WriteLine(i);
                    using (var test = new StorageCompactionTests())
                    {
                        test.ShouldDeleteCurrentJournalEvenThoughItHasAvailableSpace();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    break;
                }

            }
        }
    }
}
