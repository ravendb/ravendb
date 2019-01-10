using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Queries.Parser;
using FastTests.Voron.Backups;
using FastTests.Voron.Compaction;
using SlowTests.Authentication;
using SlowTests.Bugs.MapRedue;
using SlowTests.Client;
using SlowTests.Client.Attachments;
using SlowTests.Issues;
using SlowTests.MailingList;
using Sparrow.Logging;
using StressTests.Client.Attachments;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);

                try
                {
                    using (var test = new RavenDB_8288())
                    {
                        await test.Queries_will_work_during_index_replacements();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Console.ReadLine();
                }
            }

            return;
        }
    }
}
