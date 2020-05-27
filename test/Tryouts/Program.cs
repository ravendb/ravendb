using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using FastTests.Client.Indexing;
using FastTests.Issues;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.OngoingTasks;
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
            using var store = new DocumentStore
            {
                Certificate = new X509Certificate2(@"C:\Users\ayende\Downloads\snark.Cluster.Settings\admin.client.certificate.snark.pfx"),
                Urls = new[] {"https://a.snark.development.run"},
                Database = "test"
            }.Initialize();

            var res = store.Maintenance.Send(new GetPullReplicationTasksInfoOperation(5));
            res.Definition.Filters = new Dictionary<string, PullReplicationDefinition.FilteringOptions>();
            res.Definition.Filters[res.Definition.Certificates.First().Key] = new    PullReplicationDefinition.FilteringOptions
            {
                AllowedPaths = new []{"users/ayende/*", "user/ayende"}
            };
            store.Maintenance.Send(new PutPullReplicationAsHubOperation(res.Definition));
        }
    }
}
