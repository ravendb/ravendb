using System;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
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
