using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Queries.Parser;
using FastTests.Voron.Backups;
using FastTests.Voron.Compaction;
using SlowTests.Authentication;
using SlowTests.Bugs.MapRedue;
using SlowTests.Client;
using SlowTests.Issues;
using SlowTests.MailingList;
using Sparrow.Logging;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            LoggingSource.Instance.SetupLogMode(LogMode.Information, "C:\\testlogs");

            try
            {
                using (var test = new SlowTests.Authentication.AuthenticationLetsEncryptTests())
                {
                    await test.CanGetLetsEncryptCertificateAndRenewIt();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            
        }
    }
}
