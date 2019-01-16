using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Queries.Parser;
using FastTests.Voron.Backups;
using FastTests.Voron.Compaction;
using RachisTests.DatabaseCluster;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Authentication;
using SlowTests.Bugs.MapRedue;
using SlowTests.Client;
using SlowTests.Client.Attachments;
using SlowTests.Issues;
using SlowTests.MailingList;
using Sparrow.Logging;
using StressTests.Client.Attachments;
using Xunit;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            using (var test = new AuthenticationClusterTests())
            {
                test.CanReplaceClusterCert().Wait();
            }
            /*using (var test = new AuthenticationLetsEncryptTests())
            {
                test.CanGetLetsEncryptCertificateAndRenewIt().Wait();
            }*/
        }
    }
}
