using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Queries.Parser;
using FastTests.Voron.Backups;
using FastTests.Voron.Compaction;
using RachisTests;
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
using Sparrow.Platform;
using StressTests.Client.Attachments;
using Xunit;

namespace Tryouts
{

    public static class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 500; i++)
            {
                Console.WriteLine(i);
                try
                {
                    using (var test = new ReplicationTests())
                    {
                        test.EnsureReplicationToWatchers(false).Wait();
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
