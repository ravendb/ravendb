using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17237 : RavenTestBase
    {
        public RavenDB_17237(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task MustNotDisableThrottlingTimerOnUpdatingIndexDefinitionOfThrottledIndex()
        {
            using (var store = GetDocumentStore())
            {
                var indexDef = new Orders_ByOrderedAtAndShippedAt(storeFields: false)
                {
                    Configuration = {[RavenConfiguration.GetKey(x => x.Indexing.ThrottlingTimeInterval)] = "1000"}
                };

                await indexDef.ExecuteAsync(store);

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);

                var index = database.IndexStore.GetIndex(indexDef.IndexName);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { ShippedAt = DateTime.Now, OrderedAt = DateTime.Now });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                Assert.Equal(TimeSpan.FromSeconds(1), index._mre.ThrottlingInterval);
                Assert.NotNull(index._mre._timerTask);
                Assert.False(index._mre.Wait(100, CancellationToken.None));

                // update index def

                store.Maintenance.Send(new StopIndexingOperation());

                var updatedIndexDef = new Orders_ByOrderedAtAndShippedAt(storeFields: true)
                {
                    Configuration = {[RavenConfiguration.GetKey(x => x.Indexing.ThrottlingTimeInterval)] = "1000"}
                };

                await updatedIndexDef.ExecuteAsync(store);

                var replacementIndex = database.IndexStore.GetIndex(Constants.Documents.Indexing.SideBySideIndexNamePrefix + updatedIndexDef.IndexName);

                using (index.ForTestingPurposesOnly().CallDuringFinallyOfExecuteIndexing(() =>
                {
                    // stop the current index for a moment to ensure that a new thread will start - the one after renaming the replacement index
                    Thread.Sleep(2000);
                }))
                {
                    store.Maintenance.Send(new StartIndexingOperation());
                    Indexes.WaitForIndexing(store);
                }

                Assert.NotNull(replacementIndex._mre._timerTask);
                Assert.False(replacementIndex._mre.Wait(100, CancellationToken.None));

                using (var session = store.OpenSession())
                {
                    session.Store(new Order {ShippedAt = DateTime.Now, OrderedAt = DateTime.Now});

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
            }
        }

        private class Orders_ByOrderedAtAndShippedAt : AbstractIndexCreationTask<Order>
        {
            public Orders_ByOrderedAtAndShippedAt(bool storeFields)
            {
                Map = orders => from o in orders
                    select new {o.OrderedAt, o.ShippedAt};

                if (storeFields)
                    StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
