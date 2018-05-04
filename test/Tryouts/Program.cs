using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using SlowTests.Client.Attachments;
using SlowTests.Tests.Sorting;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new SlowTests.Cluster.ClusterTransactionTests())
                {
                    await test.CanCreateClusterTransactionRequest();
                }
            }
            
        }
    }
}
