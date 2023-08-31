using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues;

public class RavenDB_19149 : RavenTestBase
{
    public RavenDB_19149(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void LuceneOptimizeIsNotHanging()
    {
        using var store = GetDocumentStore(new Options()
        {
            RunInMemory = false,
            ModifyDatabaseRecord = record =>
            {
                //This should stop all merges.
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MaxTimeForMergesToKeepRunning)] = "0";
            }
        });

        PrepareDataOnTheServer(store, out ExampleIndex index);

        var settings = new CompactSettings
        {
            DatabaseName = store.Database,
            Documents = true,
            Indexes = new[] { index.IndexName }
        };

        var operation = store.Maintenance.Server.Send(new CompactDatabaseOperation(settings));

        operation.WaitForCompletion(TimeSpan.FromMinutes(1));
    }
    
    [Fact]
    public async Task LuceneOptimizeEndpoint()
    {
        using var store = GetDocumentStore(new Options()
        {
            RunInMemory = false,
        });

        PrepareDataOnTheServer(store, out ExampleIndex index);

        var operation = await store.Maintenance.SendAsync(new IndexOptimizeOperation(index.IndexName));
        var results = await operation.WaitForCompletionAsync<IndexOptimizeResult>(TimeSpan.FromSeconds(60));
        var indexStats = await store.Maintenance.SendAsync(new GetIndexesStatisticsOperation());
        var indexStat = indexStats.FirstOrDefault(i => string.Compare(i.Name, index.IndexName, StringComparison.InvariantCultureIgnoreCase) == 0);
        Assert.NotNull(indexStat);
        Assert.Equal(IndexState.Normal, indexStat.State);
    }

    private void PrepareDataOnTheServer(DocumentStore store, out ExampleIndex exampleIndex)
    {
        {
            using var bulkInsert = store.BulkInsert();
            var random = new Random(1241231);
            var names = Enumerable.Range(0, 10).Select(i => $"Name{i}").ToArray();
            for (int i = 0; i < 100; ++i)
            {
                bulkInsert.Store(new Test(names[random.Next(names.Length)], names[random.Next(names.Length)]));
            }
        }
        exampleIndex = new ExampleIndex();
        exampleIndex.Execute(store);
        Indexes.WaitForIndexing(store);
    }

    private record Test(string Name, string LastName);
    private class ExampleIndex : AbstractIndexCreationTask<Test>
    {
        public ExampleIndex()
        {
            Map = tests => tests.Select(i => new {Name = i.Name, LastName = i.LastName});
        }
    }
    
    private class IndexOptimizeOperation : IMaintenanceOperation<OperationIdResult>
    {
        private readonly string _indexName;

        public IndexOptimizeOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new IndexOptimizeCommand(_indexName);
        }

        private class IndexOptimizeCommand : RavenCommand<OperationIdResult>
        {
            private readonly string _indexName;

            public IndexOptimizeCommand(string indexName)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/optimize?name={Uri.EscapeDataString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.OperationIdResult(response);
            }

            public override bool IsReadRequest => true;
        }
    }
}
