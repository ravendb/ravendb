using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;

namespace Allocations.Benchmark
{
    // Reads and writes of DocumentsStorage, on the database of a test server
    public class DocumentsScenarios(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const int NumberOfDocuments = 2_000;

        public async Task GetDocumentById()
        {
            using var store = GetDocumentStore();
            await using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < NumberOfDocuments; i++)
                    await bulk.StoreAsync(new Order { Company = $"companies/{i % 50}", Freight = i }, $"orders/{i}");
            }

            var database = await GetDatabase(store.Database);
            var ids = new string[NumberOfDocuments];
            for (int i = 0; i < NumberOfDocuments; i++)
                ids[i] = $"orders/{(i * 997) % NumberOfDocuments}";

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                int i = 0;
                Measure.Report("DocumentsStorage.Get", Measure.PerOperation(() =>
                {
                    using (var document = database.DocumentsStorage.Get(context, ids[i++ % NumberOfDocuments]))
                        Assert.NotNull(document);
                }));
            }
        }

        public async Task PutNewDocumentsWithRevisions()
        {
            using var store = GetDocumentStore();
            var configured = await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { MinimumRevisionsToKeep = 5 }
            }));
            await store.Maintenance.SendAsync(new WaitForIndexNotificationOperation(configured.RaftCommandIndex!.Value));
            var database = await GetDatabase(store.Database);

            const int warmup = 1_000, count = 5_000;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenWriteTransaction())
            {
                var ids = new string[warmup + count];
                var documents = new BlittableJsonReaderObject[warmup + count];
                for (int i = 0; i < ids.Length; i++)
                {
                    ids[i] = $"orders/{i}";
                    documents[i] = context.ReadObject(new DynamicJsonValue
                    {
                        ["Company"] = $"companies/{i % 50}",
                        ["Freight"] = i,
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue { [Constants.Documents.Metadata.Collection] = "Orders" }
                    }, ids[i], BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }

                int next = 0;
                Measure.Report("DocumentsStorage.Put, new document with revisions", Measure.PerOperation(() =>
                {
                    database.DocumentsStorage.Put(context, ids[next], null, documents[next]);
                    next++;
                }, operations: count, warmup: warmup));
            }
        }

        public void HashRevisionKey()
        {
            const string version = "A:1234-Q2DQ7XoZ6kyk1q9bHoCqvw, B:99-zPm1Bgl3DE6JtqZGfJqRww, C:7-7l+0sJ4gZkKdTV2v8pS0Ag";
            using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            using (Slice.From(allocator, version, out Slice raw))
            {
                Measure.Report("RevisionsStorage.GetRevisionKeyHashSlice", Measure.PerOperation(() =>
                {
                    using (RevisionsStorage.GetRevisionKeyHashSlice(allocator, raw, out _))
                    {
                    }
                }, operations: 10_000));
            }
        }

        private class Order
        {
            public string Company { get; set; }
            public double Freight { get; set; }
        }
    }
}
