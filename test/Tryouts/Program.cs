using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Queries.Parser;
using FastTests.Voron.Backups;
using FastTests.Voron.Compaction;
using SlowTests.Authentication;
using SlowTests.Bugs.MapRedue;
using SlowTests.Client;
using SlowTests.Client.Attachments;
using FastTests.Voron;
using SlowTests.Issues;
using SlowTests.MailingList;
using Sparrow.Logging;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            try
            {
                using (var test = new SlowTests.Issues.RavenDB_5570())
                {
                   test.Doing_PUT_without_commit_should_not_cause_NRE_on_subsequent_PUTs();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            
        }
    }
}
