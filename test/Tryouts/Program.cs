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
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                //using (var test = new RavenDB_12151())
                //{
                //    //Console.WriteLine("IndexingWhenTransactionSizeLimitExceeded32");
                //    //test.IndexingWhenTransactionSizeLimitExceeded32();
                //    //Console.WriteLine("IndexingWhenScratchSpaceLimitExceeded32");
                //    //test.IndexingWhenScratchSpaceLimitExceeded32();
                //    Console.WriteLine("IndexingWhenGlobalScratchSpaceLimitExceeded32");
                //    test.IndexingWhenGlobalScratchSpaceLimitExceeded32();
                //}
                //using (var test = new SlowTests.Server.RecordingTransactionOperationsMergerTests())
                //{
                //    test.RecordingPatchWithParametersByQuery("Avi");
                //}
                //using (var test = new RachisTests.SubscriptionsFailover())
                //{
                //    await test.DistributedRevisionsSubscription(3);
                //}
                using (var test = new StressTests.Server.Replication.ExternalReplicationStressTests())
                {
                    test.ExternalReplicationShouldWorkWithSmallTimeoutStress32();
                }

            }
        }
    }
}
