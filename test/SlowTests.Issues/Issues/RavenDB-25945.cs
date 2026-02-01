using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_25945(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task CanDeleteRevisionsCount()
        {
            using var store = GetDocumentStore();
            var revisionsConfig = new RevisionsConfiguration
            {
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Orders"] = new RevisionsCollectionConfiguration
                    {
                        MinimumRevisionsToKeep = 5,
                        PurgeOnDelete = true,
                        Disabled = false
                    }
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, store.Database, revisionsConfig);

            for (int i = 0; i < 3; i++)
            {
                using var s = store.OpenAsyncSession();
                await s.StoreAsync(new Order(i), "orders/1");
                await s.SaveChangesAsync();
            }

            var db = await GetDocumentDatabaseInstanceFor(store);
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using(context.OpenReadTransaction())
            {
                var count = db.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context ,"orders/1");
                Assert.Equal(3, count);

            }

            {
                using var s = store.OpenAsyncSession();
                s.Delete("orders/1");
                await s.SaveChangesAsync();
            }

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var count = db.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context, "orders/1");
                Assert.Equal(0, count);
                var tree = context.Transaction.InnerTransaction.ReadTree("RevisionsCount");
                var header = tree.ReadHeader();
                Assert.Equal(0, header.NumberOfEntries);
            }

        }
        private record Order(int Count);
    }

}
