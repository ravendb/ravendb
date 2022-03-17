using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6967 : RavenTestBase
    {
        public RavenDB_6967(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanDeleteIndexErrors()
        {
            using (var store = GetDocumentStore())
            {
                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new DeleteIndexErrorsOperation());

                Assert.Throws<IndexDoesNotExistException>(() => store.Maintenance.Send(new DeleteIndexErrorsOperation(new[] { "DoesNotExist" })));

                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition { Name = "Index1", Maps = { "from doc in docs let x = 0 select new { Total = 3/x };" } } }));
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition { Name = "Index2", Maps = { "from doc in docs let x = 0 select new { Total = 4/x };" } } }));
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition { Name = "Index3", Maps = { "from doc in docs let x = 0 select new { Total = 5/x };" } } }));

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new DeleteIndexErrorsOperation());

                store.Maintenance.Send(new DeleteIndexErrorsOperation(new[] { "Index1", "Index2", "Index3" }));

                Assert.Throws<IndexDoesNotExistException>(() => store.Maintenance.Send(new DeleteIndexErrorsOperation(new[] { "Index1", "DoesNotExist" })));

                using (var session = store.OpenSession())
                {
                    session.Store(new Company());
                    session.Store(new Company());
                    session.Store(new Company());

                    session.SaveChanges();
                }

                Indexes.WaitForIndexingErrors(store, new [] { "Index1", "Index2", "Index3" });

                store.Maintenance.Send(new StopIndexingOperation());

                var indexErrors1 = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Index1" }));
                var indexErrors2 = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Index2" }));
                var indexErrors3 = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Index3" }));

                Assert.NotEmpty(indexErrors1.SelectMany(x => x.Errors));
                Assert.NotEmpty(indexErrors2.SelectMany(x => x.Errors));
                Assert.NotEmpty(indexErrors3.SelectMany(x => x.Errors));

                store.Maintenance.Send(new DeleteIndexErrorsOperation(new[] { "Index2" }));

                indexErrors1 = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Index1" }));
                indexErrors2 = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Index2" }));
                indexErrors3 = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Index3" }));

                Assert.NotEmpty(indexErrors1.SelectMany(x => x.Errors));
                Assert.Empty(indexErrors2.SelectMany(x => x.Errors));
                Assert.NotEmpty(indexErrors3.SelectMany(x => x.Errors));

                store.Maintenance.Send(new DeleteIndexErrorsOperation());

                indexErrors1 = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Index1" }));
                indexErrors2 = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Index2" }));
                indexErrors3 = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Index3" }));

                Assert.Empty(indexErrors1.SelectMany(x => x.Errors));
                Assert.Empty(indexErrors2.SelectMany(x => x.Errors));
                Assert.Empty(indexErrors3.SelectMany(x => x.Errors));

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }
    }
}
