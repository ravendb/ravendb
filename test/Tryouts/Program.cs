using System;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using FastTests.Client.Indexing;
using FastTests.Issues;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Replication;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using SlowTests.Issues;
using SlowTests.Voron;
using Sparrow;
using Tests.Infrastructure;
using Xunit.Sdk;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }
        
        public static async Task Main(string[] args)
        {
            using var filteredReplicationTests = new FilteredReplicationTests(new ConsoleTestOutputHelper());
            await filteredReplicationTests.Can_push_via_filtered_replication();
        }
    }
}
