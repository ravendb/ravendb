using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6967 : RavenTestBase
    {
        public RavenDB_6967(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanDeleteIndexErrors(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                RavenTestHelper.AssertNoIndexErrors(store);

                var deleteAllIndexErrors = store.Maintenance.ForTesting(() => new DeleteIndexErrorsOperation());
                await deleteAllIndexErrors.ExecuteOnAllAsync();

                var deleteIndexErrors2 = store.Maintenance.ForTesting(() => new DeleteIndexErrorsOperation(new[] { "DoesNotExist" }));
                await deleteIndexErrors2.ExecuteOnAllAsync(async task =>
                {
                    await Assert.ThrowsAsync<IndexDoesNotExistException>(() => task);
                });

                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition { Name = "Index1", Maps = { "from doc in docs let x = 0 select new { Total = 3/x };" } } }));
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition { Name = "Index2", Maps = { "from doc in docs let x = 0 select new { Total = 4/x };" } } }));
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition { Name = "Index3", Maps = { "from doc in docs let x = 0 select new { Total = 5/x };" } } }));

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                deleteAllIndexErrors = store.Maintenance.ForTesting(() => new DeleteIndexErrorsOperation());
                await deleteAllIndexErrors.ExecuteOnAllAsync();

                var deleteIndexErrors3 = store.Maintenance.ForTesting(() => new DeleteIndexErrorsOperation(new[] { "Index1", "Index2", "Index3" }));
                await deleteIndexErrors3.ExecuteOnAllAsync();

                var deleteIndexErrors4 = store.Maintenance.ForTesting(() => new DeleteIndexErrorsOperation(new[] { "Index1", "DoesNotExist" }));
                await deleteIndexErrors4.ExecuteOnAllAsync(async task =>
                {
                    await Assert.ThrowsAsync<IndexDoesNotExistException>(() => task);
                });

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                        session.Store(new Company());

                    session.SaveChanges();
                }

                Indexes.WaitForIndexingErrors(store, new[] { "Index1", "Index2", "Index3" });

                var stopIndexing = store.Maintenance.ForTesting(() => new StopIndexingOperation());
                await stopIndexing.ExecuteOnAllAsync();

                var indexErrors = store.Maintenance.ForTesting(() => new GetIndexErrorsOperation(new[] { "Index1" }));
                await indexErrors.AssertAllAsync((_, errors) =>
                {
                    Assert.NotEmpty(errors.SelectMany(x => x.Errors));
                });

                indexErrors = store.Maintenance.ForTesting(() => new GetIndexErrorsOperation(new[] { "Index2" }));
                await indexErrors.AssertAllAsync((_, errors) =>
                {
                    Assert.NotEmpty(errors.SelectMany(x => x.Errors));
                });

                indexErrors = store.Maintenance.ForTesting(() => new GetIndexErrorsOperation(new[] { "Index3" }));
                await indexErrors.AssertAllAsync((_, errors) =>
                {
                    Assert.NotEmpty(errors.SelectMany(x => x.Errors));
                });

                var deleteIndexErrorsForIndex2 = store.Maintenance.ForTesting(() => new DeleteIndexErrorsOperation(new[] { "Index2" }));
                await deleteIndexErrorsForIndex2.ExecuteOnAllAsync();

                indexErrors = store.Maintenance.ForTesting(() => new GetIndexErrorsOperation(new[] { "Index1" }));
                await indexErrors.AssertAllAsync((_, errors) =>
                {
                    Assert.NotEmpty(errors.SelectMany(x => x.Errors));
                });

                indexErrors = store.Maintenance.ForTesting(() => new GetIndexErrorsOperation(new[] { "Index2" }));
                await indexErrors.AssertAllAsync((_, errors) =>
                {
                    Assert.Empty(errors.SelectMany(x => x.Errors));
                });

                indexErrors = store.Maintenance.ForTesting(() => new GetIndexErrorsOperation(new[] { "Index3" }));
                await indexErrors.AssertAllAsync((_, errors) =>
                {
                    Assert.NotEmpty(errors.SelectMany(x => x.Errors));
                });

                deleteAllIndexErrors = store.Maintenance.ForTesting(() => new DeleteIndexErrorsOperation());
                await deleteAllIndexErrors.ExecuteOnAllAsync();

                indexErrors = store.Maintenance.ForTesting(() => new GetIndexErrorsOperation(new[] { "Index1" }));
                await indexErrors.AssertAllAsync((_, errors) =>
                {
                    Assert.Empty(errors.SelectMany(x => x.Errors));
                });

                indexErrors = store.Maintenance.ForTesting(() => new GetIndexErrorsOperation(new[] { "Index2" }));
                await indexErrors.AssertAllAsync((_, errors) =>
                {
                    Assert.Empty(errors.SelectMany(x => x.Errors));
                });

                indexErrors = store.Maintenance.ForTesting(() => new GetIndexErrorsOperation(new[] { "Index3" }));
                await indexErrors.AssertAllAsync((_, errors) =>
                {
                    Assert.Empty(errors.SelectMany(x => x.Errors));
                });

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }
    }
}
