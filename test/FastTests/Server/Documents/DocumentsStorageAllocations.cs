using System;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents
{
    public class DocumentsStorageAllocations(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const int NumberOfDocuments = 2_000;
        private const int NumberOfOperations = 20_000;

        [RavenFact(RavenTestCategory.Core)]
        public async Task Get_document_by_id()
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
                AssertAtMost(680, "DocumentsStorage.Get", MeasurePerOperation(() =>
                {
                    using (var document = database.DocumentsStorage.Get(context, ids[i++ % NumberOfDocuments]))
                        Assert.NotNull(document);
                }));
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task Put_new_documents_with_revisions()
        {
            using var store = GetDocumentStore();
            await RevisionsHelper.SetupRevisionsAsync(store);
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

                for (int i = 0; i < warmup; i++)
                    database.DocumentsStorage.Put(context, ids[i], null, documents[i]);

                var before = GC.GetAllocatedBytesForCurrentThread();
                for (int i = warmup; i < ids.Length; i++)
                    database.DocumentsStorage.Put(context, ids[i], null, documents[i]);

                AssertAtMost(2_100, "DocumentsStorage.Put, new document with revisions", (double)(GC.GetAllocatedBytesForCurrentThread() - before) / count);
            }
        }

        private void AssertAtMost(int limit, string name, double bytesPerOperation)
        {
            Output.WriteLine($"{name,-40} {bytesPerOperation,8:F1} bytes/op");
#if !DEBUG
            Assert.True(bytesPerOperation <= limit, $"{name} allocated {bytesPerOperation:F1} bytes/op, expected at most {limit}");
#endif
        }

        private static double MeasurePerOperation(Action action)
        {
            for (int i = 0; i < 1_000; i++) // warm up so JIT and pool filling are excluded from the measurement
                action();

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < NumberOfOperations; i++)
                action();

            return (double)(GC.GetAllocatedBytesForCurrentThread() - before) / NumberOfOperations;
        }

        private class Order
        {
            public string Company { get; set; }
            public double Freight { get; set; }
        }
    }
}
